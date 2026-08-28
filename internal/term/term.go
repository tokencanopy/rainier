package term

type Color struct {
	Mode uint8 // 0=default, 1=ansi256, 2=rgb
	Idx  uint8
	R, G, B uint8
}

type Cell struct {
	R          rune
	FG, BG     Color
	Bold       bool
	Underline  bool
	Reverse    bool
}

type Screen struct {
	Cells        [][]Cell // [row][col]
	CursorX      int
	CursorY      int
	CursorHidden bool
	Alt          bool
	Cols, Rows   int
}

type Emulator interface {
	Feed(p []byte)
	Screen() Screen
	Resize(cols, rows int)
}
