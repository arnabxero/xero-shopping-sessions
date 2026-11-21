//! Persons: the connected components of the link graph, built on all
//! threads.
//!
//! Every linked ID gets a slot of one open-addressing table (inserted with a
//! compare-and-swap on the key), and the slot's value serves as the
//! union-find parent. A union links the root with the larger key under the
//! root with the smaller key (a compare-and-swap that only succeeds on a
//! root), so every tree's root is its smallest key and the result does not
//! depend on the order of the unions. Finally each slot's value becomes the
//! key of its root: the person's name.

use crate::keys;
use crate::map::mix;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const EMPTY: u64 = u64::MAX;

/// Map from every linked ID to its person's name (the byte-wise smallest ID
/// of its component).
pub struct Persons {
    /// key, value
    slots: Vec<[AtomicU64; 2]>,
    mask: usize,
}

struct Ptr<T>(*mut T);
unsafe impl<T> Send for Ptr<T> {}
unsafe impl<T> Sync for Ptr<T> {}

impl Persons {
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u64> {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            let key = s[0].load(Relaxed);
            if key == k {
                return Some(s[1].load(Relaxed));
            }
            if key == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Hint the cache line holding the first probe slot of `k`.
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        let i = mix(k) as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
    }

    #[inline(always)]
    fn key(&self, i: usize) -> u64 {
        self.slots[i][0].load(Relaxed)
    }

    #[inline(always)]
    fn parent(&self, i: usize) -> usize {
        self.slots[i][1].load(Relaxed) as usize
    }

    /// The slot of `k`, inserting it if absent.
    #[inline(always)]
    fn insert(&self, k: u64) -> usize {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let cur = self.slots[i][0].load(Relaxed);
            if cur == k {
                return i;
            }
            if cur == EMPTY {
                match self.slots[i][0].compare_exchange(EMPTY, k, Relaxed, Relaxed) {
                    Ok(_) => return i,
                    Err(now) if now == k => return i,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    /// The root of `x`, halving the path on the way.
    #[inline(always)]
    fn find(&self, mut x: usize) -> usize {
        loop {
            let p = self.parent(x);
            if p == x {
                return x;
            }
            let gp = self.parent(p);
            if gp != p {
                // gp is an ancestor of x for good (links never undo), so
                // storing it is safe whatever other threads do.
                self.slots[x][1].store(gp as u64, Relaxed);
            }
            x = gp;
        }
    }

    /// The root of `x`, without writing.
    #[inline(always)]
    fn root(&self, mut x: usize) -> usize {
        loop {
            let p = self.parent(x);
            if p == x {
                return x;
            }
            x = p;
        }
    }

    fn union(&self, a: usize, b: usize) {
        loop {
            let (ra, rb) = (self.find(a), self.find(b));
            if ra == rb {
                return;
            }
            let (small, big) = if keys::cmp(self.key(ra), self.key(rb)).is_lt() { (ra, rb) } else { (rb, ra) };
            // Succeeds only while `big` is still a root.
            if self.slots[big][1].compare_exchange(big as u64, small as u64, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }
}

/// Split 0..n into about `parts` ranges.
fn ranges(n: usize, parts: usize) -> Vec<(usize, usize)> {
    let step = n.div_ceil(parts.max(1)).max(1);
    (0..n).step_by(step).map(|s| (s, (s + step).min(n))).collect()
}

/// The persons of the link pairs (keys from keys::key), on `threads` threads.
pub fn build(pairs: &[Vec<(u64, u64)>], threads: usize) -> Persons {
    let par = |n: usize, f: &(dyn Fn(usize, usize) + Sync)| {
        let rs = ranges(n, threads * 4);
        crate::par_tasks(threads, rs.len(), |_, t| f(rs[t].0, rs[t].1));
    };
    let n: usize = pairs.iter().map(|v| v.len()).sum();
    // At most 2n IDs: load factor at most 1/2.
    let cap = (4 * n).max(16).next_power_of_two();
    let mut slots: Vec<[AtomicU64; 2]> = Vec::with_capacity(cap);
    {
        let base = Ptr(slots.as_mut_ptr());
        let base = &base;
        par(cap, &|s, e| {
            for i in s..e {
                unsafe { base.0.add(i).write([AtomicU64::new(EMPTY), AtomicU64::new(i as u64)]) };
            }
        });
    }
    unsafe { slots.set_len(cap) };
    let m = Persons { slots, mask: cap - 1 };

    // The pairs, flattened, as slots.
    let flat: Vec<(u64, u64)> = pairs.iter().flatten().copied().collect();
    let mut dense: Vec<(u32, u32)> = Vec::with_capacity(n);
    {
        let out = Ptr(dense.as_mut_ptr());
        let (out, m, flat) = (&out, &m, &flat);
        par(n, &|s, e| {
            const AHEAD: usize = 16;
            for i in s..e {
                if i + AHEAD < e {
                    let (a, b) = flat[i + AHEAD];
                    m.prefetch(a);
                    m.prefetch(b);
                }
                let (a, b) = flat[i];
                let d = (m.insert(a) as u32, m.insert(b) as u32);
                unsafe { out.0.add(i).write(d) };
            }
        });
    }
    unsafe { dense.set_len(n) };
    {
        let (m, dense) = (&m, &dense);
        par(n, &|s, e| {
            for &(a, b) in &dense[s..e] {
                m.union(a as usize, b as usize);
            }
        });
    }
    // Every slot to its root (the only writes: each slot's own value)...
    {
        let m = &m;
        par(cap, &|s, e| {
            for i in s..e {
                if m.key(i) != EMPTY {
                    let r = m.root(i);
                    m.slots[i][1].store(r as u64, Relaxed);
                }
            }
        });
    }
    // ...then to its root's key.
    {
        let m = &m;
        par(cap, &|s, e| {
            for i in s..e {
                if m.key(i) != EMPTY {
                    let r = m.parent(i);
                    m.slots[i][1].store(m.key(r), Relaxed);
                }
            }
        });
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Components and names match a plain sequential union-find.
    #[test]
    fn matches_sequential() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..40 {
            let ids = 50 + (rnd() % 5000) as u64;
            let np = (rnd() % (2 * ids)) as usize;
            let key = |x: u64| keys::pack(format!("u{}", x * 7919 % 100_003).as_bytes()).unwrap();
            let mut pairs: Vec<Vec<(u64, u64)>> = vec![Vec::new(); 1 + (rnd() % 7) as usize];
            let mut all = Vec::new();
            for i in 0..np {
                let (a, b) = (key(rnd() % ids), key(rnd() % ids));
                let k = i % pairs.len();
                pairs[k].push((a, b));
                all.push((a, b));
            }
            let m = build(&pairs, 1 + round % 8);
            // sequential reference
            let mut parent: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
            fn f(p: &mut std::collections::HashMap<u64, u64>, x: u64) -> u64 {
                let mut r = x;
                while p[&r] != r {
                    r = p[&r];
                }
                r
            }
            for &(a, b) in &all {
                parent.entry(a).or_insert(a);
                parent.entry(b).or_insert(b);
                let (ra, rb) = (f(&mut parent, a), f(&mut parent, b));
                if ra != rb {
                    let (s, g) = if ra < rb { (ra, rb) } else { (rb, ra) };
                    parent.insert(g, s);
                }
            }
            let ks: Vec<u64> = parent.keys().copied().collect();
            for k in ks {
                let want = f(&mut parent, k);
                assert_eq!(m.get(k), Some(want), "round {round}");
            }
            assert_eq!(m.get(key(ids + 1)), None);
        }
    }
}
