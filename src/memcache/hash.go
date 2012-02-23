package memcache

import (
	"crypto/md5"
	"hash/crc32"
	"hash/fnv"
)

type HashMethod func(v []byte) (h uint32)

func md5hash(src []byte) uint32 {
	hash := md5.New()
	hash.Write(src)
	s := hash.Sum(nil)
	return (uint32(s[3]) << 24) | (uint32(s[2]) << 16) | (uint32(s[1]) << 8) | uint32(s[0])
}

func crc32hash(s []byte) uint32 {
	hash := crc32.NewIEEE()
	hash.Write(s)
	return hash.Sum32()
}

func fnv1a(s []byte) uint32 {
	h := fnv.New32a()
	h.Write(s)
	return h.Sum32()
}

const FNV1A_PRIME uint32 = 0x01000193
const FNV1A_INIT uint32 = 0x811c9dc5

// Bugy version of fnv1a
func fnv1a1(s []byte) uint32 {
	h := FNV1A_INIT
	for _, c := range s {
		h ^= uint32(int8(c))
		h *= FNV1A_PRIME
	}
	return h
}

var hashMethods = map[string]HashMethod{
	"fnv1a":  fnv1a,
	"fnv1a1": fnv1a1,
	"crc32":  crc32hash,
	"md5":    md5hash,
}
