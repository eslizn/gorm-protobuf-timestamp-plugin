package timestamp

import (
	"context"
	"database/sql"
	"reflect"
	"slices"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type Protobuf struct {
}

func (*Protobuf) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, dbValue any) error {
	switch val := dbValue.(type) {
	case time.Time:
		return field.Set(ctx, dst, timestamppb.New(val))
	case nil:
		return nil
	default:
		return errors.Errorf("invalid database type: %T", dbValue)
	}
}

func (*Protobuf) Value(ctx context.Context, field *schema.Field, dst reflect.Value, fieldValue any) (any, error) {
	switch val := fieldValue.(type) {
	case *timestamppb.Timestamp:
		if val == nil {
			return &sql.NullTime{}, nil
		}
		return &sql.NullTime{
			Time:  val.AsTime(),
			Valid: true,
		}, nil
	default:
		return nil, errors.Errorf("invalid source type: %T", fieldValue)
	}
}

func (p *Protobuf) Name() string {
	return "timestamppb"
}

func (p *Protobuf) Initialize(db *gorm.DB) error {
	var check = []error{
		db.Callback().Delete().Before("gorm:before_delete").Register(p.Name(), p.BeforeDelete),
		db.Callback().Create().Before("gorm:before_create").Register(p.Name(), p.BeforeCreate),
		db.Callback().Update().Before("gorm:update").Register(p.Name(), p.BeforeUpdate),
		db.Callback().Query().Before("gorm:query").Register(p.Name(), p.BeforeQuery),
	}
	for k := range check {
		if check[k] != nil {
			return check[k]
		}
	}
	return nil
}

func (p *Protobuf) BeforeCreate(db *gorm.DB) {
	p.SetIfNil(db, db.Statement.ReflectValue, "CreatedAt", "UpdatedAt")
}

func (p *Protobuf) BeforeUpdate(db *gorm.DB) {
	p.SetIfNil(db, db.Statement.ReflectValue, "UpdatedAt")
}

func (p *Protobuf) BeforeQuery(db *gorm.DB) {
	if db.Statement.Schema == nil || db.Statement.Unscoped {
		return
	}
	field, ok := db.Statement.Schema.FieldsByName["DeletedAt"]
	if !ok || field.FieldType != reflect.TypeOf(&timestamppb.Timestamp{}) {
		return
	}

	// Modify query to add deleteAt is NULL
	db = db.Where(db.Statement.Table + "." + field.DBName + " IS NULL")
}

func (p *Protobuf) BeforeDelete(db *gorm.DB) {
	if db.Statement.Schema == nil || db.Statement.Unscoped {
		return
	}
	field, ok := db.Statement.Schema.FieldsByName["DeletedAt"]
	if !ok || field.FieldType != reflect.TypeOf(&timestamppb.Timestamp{}) {
		return
	}

	// Modify query to update instead of delete the record
	db.Statement.AddClause(clause.Set{{Column: clause.Column{Name: field.DBName}, Value: time.Now()}})
	db.Statement.SetColumn(field.DBName, timestamppb.Now(), true)
	db.Statement.AddClauseIfNotExists(clause.Update{})
	db.Statement.Build(db.Statement.DB.Callback().Update().Clauses...)
}

func (p *Protobuf) SetIfNil(db *gorm.DB, value reflect.Value, fields ...string) {
	if db.Statement.Schema == nil || db.Statement.Unscoped || len(fields) < 1 {
		return
	}
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		for idx := 0; idx < value.Len(); idx++ {
			p.SetIfNil(db, value.Index(idx), fields...)
		}
	case reflect.Struct:
		for idx := 0; idx < value.NumField(); idx++ {
			if slices.Index(fields, value.Type().Field(idx).Name) < 0 {
				continue
			}
			if value.Field(idx).IsNil() {
				value.Field(idx).Set(reflect.ValueOf(timestamppb.Now()))
			}
		}
	case reflect.Ptr:
		p.SetIfNil(db, value.Elem(), fields...)
	}
}

func init() {
	schema.RegisterSerializer("timestamppb", &Protobuf{})
}
