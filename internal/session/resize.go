package session

type Size struct{ Cols, Rows int }

func EffectiveSize(viewers []Size) (Size, bool) {
	if len(viewers) == 0 { return Size{}, false }
	eff := viewers[0]
	for _, v := range viewers[1:] {
		if v.Cols < eff.Cols { eff.Cols = v.Cols }
		if v.Rows < eff.Rows { eff.Rows = v.Rows }
	}
	return eff, true
}
