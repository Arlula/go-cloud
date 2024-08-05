package awsdynamodb

import (
	dyn2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyn "github.com/aws/aws-sdk-go/service/dynamodb"
)

type tableDescription struct {
	LocalSecondaryIndexes  []tableIndex
	GlobalSecondaryIndexes []tableIndex
}

type tableIndex struct {
	IndexName  string
	KeySchema  []keySchemaElement
	Projection projection
}

type keySchemaElement struct {
	AttributeName string
	KeyType       string
}

type projection struct {
	NonKeyAttributes []string
	ProjectionType   string
}

func tableDescriptionFromV1Output(out *dyn.DescribeTableOutput) *tableDescription {
	desc := &tableDescription{
		LocalSecondaryIndexes:  make([]tableIndex, len(out.Table.LocalSecondaryIndexes)),
		GlobalSecondaryIndexes: make([]tableIndex, len(out.Table.GlobalSecondaryIndexes)),
	}

	for i, ind := range out.Table.LocalSecondaryIndexes {
		ix := tableIndex{
			IndexName: *ind.IndexName,
			KeySchema: make([]keySchemaElement, len(ind.KeySchema)),
			Projection: projection{
				NonKeyAttributes: make([]string, len(ind.Projection.NonKeyAttributes)),
				ProjectionType:   *ind.Projection.ProjectionType,
			},
		}
		for j, str := range ind.Projection.NonKeyAttributes {
			ix.Projection.NonKeyAttributes[j] = *str
		}
		for j, sch := range ind.KeySchema {
			ix.KeySchema[j] = keySchemaElement{
				AttributeName: *sch.AttributeName,
				KeyType:       *sch.KeyType,
			}
		}
		desc.LocalSecondaryIndexes[i] = ix
	}

	for i, ind := range out.Table.GlobalSecondaryIndexes {
		ix := tableIndex{
			IndexName: *ind.IndexName,
			KeySchema: make([]keySchemaElement, len(ind.KeySchema)),
			Projection: projection{
				NonKeyAttributes: make([]string, len(ind.Projection.NonKeyAttributes)),
				ProjectionType:   *ind.Projection.ProjectionType,
			},
		}
		for j, str := range ind.Projection.NonKeyAttributes {
			ix.Projection.NonKeyAttributes[j] = *str
		}
		for j, sch := range ind.KeySchema {
			ix.KeySchema[j] = keySchemaElement{
				AttributeName: *sch.AttributeName,
				KeyType:       *sch.KeyType,
			}
		}
		desc.GlobalSecondaryIndexes[i] = ix
	}

	return desc
}

func tableDescriptionFromV2Output(out *dyn2.DescribeTableOutput) *tableDescription {
	desc := &tableDescription{
		LocalSecondaryIndexes:  make([]tableIndex, len(out.Table.LocalSecondaryIndexes)),
		GlobalSecondaryIndexes: make([]tableIndex, len(out.Table.GlobalSecondaryIndexes)),
	}

	for i, ind := range out.Table.LocalSecondaryIndexes {
		ix := tableIndex{
			IndexName: *ind.IndexName,
			KeySchema: make([]keySchemaElement, len(ind.KeySchema)),
			Projection: projection{
				NonKeyAttributes: ind.Projection.NonKeyAttributes,
				ProjectionType:   string(ind.Projection.ProjectionType),
			},
		}
		for j, sch := range ind.KeySchema {
			ix.KeySchema[j] = keySchemaElement{
				AttributeName: *sch.AttributeName,
				KeyType:       string(sch.KeyType),
			}
		}
		desc.LocalSecondaryIndexes[i] = ix
	}

	for i, ind := range out.Table.GlobalSecondaryIndexes {
		ix := tableIndex{
			IndexName: *ind.IndexName,
			KeySchema: make([]keySchemaElement, len(ind.KeySchema)),
			Projection: projection{
				NonKeyAttributes: ind.Projection.NonKeyAttributes,
				ProjectionType:   string(ind.Projection.ProjectionType),
			},
		}
		for j, sch := range ind.KeySchema {
			ix.KeySchema[j] = keySchemaElement{
				AttributeName: *sch.AttributeName,
				KeyType:       string(sch.KeyType),
			}
		}
		desc.GlobalSecondaryIndexes[i] = ix
	}

	return desc
}
