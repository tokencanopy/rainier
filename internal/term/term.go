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

	// Width is the cell's mono-spaced column width: 1 for an ordinary cell,
	// 2 for the lead cell of a wide (e.g. CJK, emoji) rune, and 0 for the
	// placeholder cell immediately following a width-2 cell. Serialize skips
	// Width-0 cells entirely — the wide rune written for the lead cell
	// already advances a real terminal's cursor across both columns, so
	// emitting anything for the placeholder would shift everything after it
	// one column to the right.
	Width int
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
