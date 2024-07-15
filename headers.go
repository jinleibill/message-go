package message_go

import "fmt"

type Headers map[string]string

func (h Headers) Has(key string) bool {
	_, ok := h[key]
	return ok
}

func (h Headers) Get(key string) (string, error) {
	value, exists := h[key]
	if !exists {
		return "", fmt.Errorf("header %s not found", key)
	}
	return value, nil
}

func (h Headers) Set(key, value string) {
	h[key] = value
}
