package terraform

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"net/netip"
	"unicode/utf8"

	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/gocty"
)

// Terraform functions that go-cty's standard library lacks but configs use often. They are pure,
// so pogi can run them like Terraform does.
var extraFunctions = map[string]function.Function{
	"cidrsubnet":   cidrSubnetFunc,
	"cidrhost":     cidrHostFunc,
	"cidrnetmask":  cidrNetmaskFunc,
	"base64encode": base64EncodeFunc,
	"base64decode": base64DecodeFunc,
}

// quietFunctions are functions pogi doesn't run (they read other files, hash, or depend on the
// clock) and whose results are never cost inputs, so their Unknown values get no warning.
var quietFunctions = map[string]bool{
	"file": true, "filebase64": true, "fileexists": true, "fileset": true, "templatefile": true,
	"md5": true, "sha1": true, "sha256": true, "sha512": true, "bcrypt": true, "base64sha256": true,
	"base64sha512": true, "filemd5": true, "filesha256": true, "uuid": true, "uuidv5": true,
	"timestamp": true, "plantimestamp": true, "base64gzip": true,
}

func str(s string) *function.Parameter { return &function.Parameter{Name: s, Type: cty.String} }
func num(s string) *function.Parameter { return &function.Parameter{Name: s, Type: cty.Number} }

// prefix parses a CIDR the way Terraform does: the address may have host bits set.
func prefix(s string) (netip.Prefix, error) {
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return p, fmt.Errorf("invalid CIDR %q", s)
	}
	return p, nil
}

// hostAddr returns the address n places into p; a negative n counts back from the end.
func hostAddr(p netip.Prefix, n *big.Int) (netip.Addr, error) {
	p = p.Masked()
	bits := p.Addr().BitLen()
	size := new(big.Int).Lsh(big.NewInt(1), uint(bits-p.Bits()))
	if n.Sign() < 0 {
		n = new(big.Int).Add(size, n)
	}
	if n.Sign() < 0 || n.Cmp(size) >= 0 {
		return netip.Addr{}, fmt.Errorf("prefix %s has no host number %s", p, n)
	}
	v := new(big.Int).Add(new(big.Int).SetBytes(p.Addr().AsSlice()), n)
	b := v.FillBytes(make([]byte, bits/8))
	a, _ := netip.AddrFromSlice(b)
	return a, nil
}

var cidrSubnetFunc = function.New(&function.Spec{
	Params: []function.Parameter{*str("prefix"), *num("newbits"), *num("netnum")},
	Type:   function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		p, err := prefix(args[0].AsString())
		if err != nil {
			return cty.UnknownVal(cty.String), err
		}
		var newbits int
		if err := gocty.FromCtyValue(args[1], &newbits); err != nil {
			return cty.UnknownVal(cty.String), err
		}
		bits := p.Bits() + newbits
		if newbits < 0 || bits > p.Addr().BitLen() {
			return cty.UnknownVal(cty.String), fmt.Errorf("cannot extend %s by %d bits", p, newbits)
		}
		netnum := args[2].AsBigFloat()
		n, _ := netnum.Int(nil)
		if n.Sign() < 0 || n.BitLen() > newbits {
			return cty.UnknownVal(cty.String), fmt.Errorf("prefix extension of %d bits has no network %s", newbits, n)
		}
		a, err := hostAddr(p, new(big.Int).Lsh(n, uint(p.Addr().BitLen()-bits)))
		if err != nil {
			return cty.UnknownVal(cty.String), err
		}
		return cty.StringVal(netip.PrefixFrom(a, bits).String()), nil
	},
})

var cidrHostFunc = function.New(&function.Spec{
	Params: []function.Parameter{*str("prefix"), *num("hostnum")},
	Type:   function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		p, err := prefix(args[0].AsString())
		if err != nil {
			return cty.UnknownVal(cty.String), err
		}
		n, _ := args[1].AsBigFloat().Int(nil)
		a, err := hostAddr(p, n)
		if err != nil {
			return cty.UnknownVal(cty.String), err
		}
		return cty.StringVal(a.String()), nil
	},
})

var cidrNetmaskFunc = function.New(&function.Spec{
	Params: []function.Parameter{*str("prefix")},
	Type:   function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		p, err := prefix(args[0].AsString())
		if err != nil {
			return cty.UnknownVal(cty.String), err
		}
		if !p.Addr().Is4() {
			return cty.UnknownVal(cty.String), fmt.Errorf("cidrnetmask works on IPv4 prefixes only")
		}
		m := ^uint32(0) << (32 - p.Bits())
		if p.Bits() == 0 {
			m = 0
		}
		return cty.StringVal(netip.AddrFrom4([4]byte{byte(m >> 24), byte(m >> 16), byte(m >> 8), byte(m)}).String()), nil
	},
})

var base64EncodeFunc = function.New(&function.Spec{
	Params: []function.Parameter{*str("str")},
	Type:   function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		return cty.StringVal(base64.StdEncoding.EncodeToString([]byte(args[0].AsString()))), nil
	},
})

var base64DecodeFunc = function.New(&function.Spec{
	Params: []function.Parameter{*str("str")},
	Type:   function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		b, err := base64.StdEncoding.DecodeString(args[0].AsString())
		if err != nil || !utf8.Valid(b) {
			return cty.UnknownVal(cty.String), fmt.Errorf("base64decode: not valid base64 of UTF-8 text")
		}
		return cty.StringVal(string(b)), nil
	},
})
