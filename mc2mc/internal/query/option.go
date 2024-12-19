package query

type Option func(*Builder)

func WithMethod(method Method) Option {
	return func(b *Builder) {
		b.method = method
	}
}

func WithDestination(tableID string) Option {
	return func(b *Builder) {
		b.destinationTableID = tableID
	}
}

func WithColumnOrder() Option {
	return func(b *Builder) {
		b.orderedColumns = []string{}
	}
}

func WithAutoPartition(enable bool) Option {
	return func(b *Builder) {
		b.enableAutoPartition = enable
	}
}

func WithPartitionValue(enable bool) Option {
	return func(b *Builder) {
		b.enablePartitionValue = enable
	}
}

func WithOverridedValue(field, value string) Option {
	return func(b *Builder) {
		if b.overridedValues == nil {
			b.overridedValues = make(map[string]string)
		}
		b.overridedValues[field] = value
	}
}
