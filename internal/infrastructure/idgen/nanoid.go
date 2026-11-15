// Package idgen implements domain.IDGenerator with nanoid.
package idgen

import (
	"crypto/rand"
	"fmt"

	"example.com/scheduler/internal/domain"
)

// URL-safe nanoid alphabet: 9 chars ≈ 54 bits of entropy.
const AlphabetURLSafe = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type Nanoid struct {
	alphabet string
	mask     int
	step     int
}

var _ domain.IDGenerator = (*Nanoid)(nil)

func NewNanoid() *Nanoid {
	n := len(AlphabetURLSafe)
	mask := 1
	for mask < n-1 {
		mask = (mask << 1) | 1
	}
	return &Nanoid{alphabet: AlphabetURLSafe, mask: mask, step: (domain.IDLen*mask*16)/(n*10) + 1}
}

// NewID uses crypto/rand with rejection sampling (no modulo bias), as reference nanoid does.
func (g *Nanoid) NewID() (string, error) {
	id := make([]byte, 0, domain.IDLen)
	buf := make([]byte, g.step)
	for {
		if _, err := rand.Read(buf); err != nil {
			return "", fmt.Errorf("nanoid: %w", err)
		}
		for _, b := range buf {
			if idx := int(b) & g.mask; idx < len(g.alphabet) {
				id = append(id, g.alphabet[idx])
				if len(id) == domain.IDLen {
					return string(id), nil
				}
			}
		}
	}
}
