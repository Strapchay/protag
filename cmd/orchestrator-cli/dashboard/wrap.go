package dashboard

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

func clampWidth(width int) int {
	if width < 1 {
		return 1
	}
	return width
}

func contentWidth(width, paddingAndBorder int) int {
	return clampWidth(width - paddingAndBorder)
}

func wrapPlain(text string, width int) string {
	width = clampWidth(width)
	var out []string
	for _, line := range strings.Split(text, "\n") {
		out = append(out, wrapPlainLine(line, width)...)
	}
	return strings.Join(out, "\n")
}

func wrapPlainLine(line string, width int) []string {
	width = clampWidth(width)
	if line == "" {
		return []string{""}
	}
	var out []string
	var current strings.Builder
	for _, r := range line {
		current.WriteRune(r)
		if lipgloss.Width(current.String()) >= width {
			out = append(out, current.String())
			current.Reset()
		}
	}
	if current.Len() > 0 {
		out = append(out, current.String())
	}
	return out
}

func truncateToWidth(text string, width int) string {
	width = clampWidth(width)
	if lipgloss.Width(text) <= width {
		return text
	}
	if width <= 1 {
		return "…"
	}
	var b strings.Builder
	for _, r := range text {
		next := b.String() + string(r)
		if lipgloss.Width(next)+1 > width {
			break
		}
		b.WriteRune(r)
	}
	return b.String() + "…"
}
