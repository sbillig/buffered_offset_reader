use std::cmp::{max, min};
use std::ops;

pub type Range = ops::Range<usize>;

pub trait RangeExt {
    fn empty(&self) -> bool; // is_empty() is in nightly
    fn intersect(&self, b: &Range) -> Range;
    fn shift_left(&self, n: usize) -> Range;
    fn shift_right(&self, n: usize) -> Range;
}

impl RangeExt for Range {
    fn empty(&self) -> bool {
        self.start >= self.end
    }

    fn intersect(&self, b: &Range) -> Range {
        let start = max(self.start, b.start);
        let end = min(self.end, b.end);

        if end <= start {
            0..0
        } else {
            start..end
        }
    }

    fn shift_left(&self, n: usize) -> Range {
        assert!(n <= self.start);
        (self.start - n)..(self.end - n)
    }

    fn shift_right(&self, n: usize) -> Range {
        (self.start + n)..(self.end + n)
    }
}

// pub fn contains(&self, b: &Range) -> bool {
//     self.offset <= b.offset && self.end() >= b.end()
// }

#[cfg(test)]
mod tests {
    use crate::range::*;

    #[test]
    fn intersect_disjoint() {
        let a = 4..14;
        let b = 16..21;
        assert!(a.intersect(&b).empty());
        assert!(b.intersect(&a).empty());
    }

    #[test]
    fn intersect_subset() {
        let a = 2..22;
        let b = 4..14;
        assert_eq!(a.intersect(&b), b);
        assert_eq!(b.intersect(&a), b);
        assert_eq!(a.intersect(&a), a);
        assert_eq!(b.intersect(&b), b);
    }

    #[test]
    fn intersect_partial() {
        let a = 2..20;
        let b = 10..30;
        let i = 10..20;
        assert_eq!(a.intersect(&b), i);
        assert_eq!(b.intersect(&a), i);
    }

    #[test]
    fn shift() {
        assert_eq!((10..20).shift_left(5), 5..15);
        assert_eq!((0..5).shift_right(10), 10..15);
    }
}
