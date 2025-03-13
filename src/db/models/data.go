package models

type ChangeData struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnValues []interface{} `json:"columnvalues"`
	OldKeys      struct {
		KeyNames  []string      `json:"keynames"`
		KeyValues []interface{} `json:"keyvalues"`
	} `json:"oldkeys"`
}
