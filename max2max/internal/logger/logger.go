package logger

import (
	"log/slog"
	"os"
)

func NewLogger(logLevel string) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return nil, err
	}

	writter := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(writter), nil
}
