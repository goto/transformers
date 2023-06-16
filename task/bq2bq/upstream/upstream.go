package upstream

type Upstream struct {
	Resource  Resource
	Upstreams []*Upstream
}
