# API Validation

All fields of all APIs must be validated.  This is especially critical for the
main "resource oriented" API because (once we hit 1.0) when bad data has been
written to a database, we can't easily fix it.  Retroactively fixing validation
to make it stricter will break users.

## Tooling

We use the Kubernetes
[validation-gen](https://github.com/kubernetes/code-generator/tree/master/cmd/validation-gen)
tool to generate validation code for our APIs.  It is driven off of "tags"
written as comments in Go code.  Protobuf comments are copied directly into the
generated Go code, so we can use them to drive validation-gen.

We call this "declarative validation" or "DV" for short.  Almost all validation
should be done with DV.  Any case which does not should be considered rare and
exceptional.

## DV Tags

All validation tags follow the same basic formats:

```
// +k8s:<tag-name>             # no payload
// +k8s:<tag-name>=<tag-value> # with payload
```

Some tags have payloads and some do not. Sometimes the payload is
optional.  Run `./hack/run-tool.sh validation-gen --docs` to see some
documentation for the tags that validation-gen supports.

All tags support optional EOL comments, using a `#` character.

Some tags take arguments.  Single-arguments can be either named or unnamed, and
multi-arguments are always named.  The sytax looks like:

```
// +k8s:<tag-name>(<value>)                        # unnamed single argument
// +k8s:<tag-name>(<arg>: <val>)                   # single named argument
// +k8s:<tag-name>(<arg1>: <val1>, <arg2>: <val2>) # multiple arguments
```

Arguments can be strings, numbers, or booleans.  Strings may be double-quoted,
backtick-quoted, or unquoted, but if they are unquoted, they must match the
regex `[A-Za-z_][A-Za-z0-9_-.]*`.

### Nested tags

Some tags take other tags as payloads.  The syntax looks like:

```
// +k8s:<tag-name>=+k8s:<other-tag-name>
```

This nesting may continue several levels deep.  Not all tags support being
nested.

### Attachment points

Tags can be attached either to a field, to a struct, or to any other type
definion (which protobuf does not support, so we don't use it).  Field-attached
validation tags define the rules for that specific field, but apply to every
use of the enclosing struct.  Struct-attached validation tags define the rules
for the struct itself, and apply to every use of the struct.

### Tag ordering

In general tags are not order-dependent, but some tags are mutually exclusive.
Some tags are "short-circuiting" and will prevent other tags from being
evaluated.  For example, if a field is marked as `required`, then it will not
be validated against most other tags if it is not specified.

## Be strict!

It is easier to loosen validation than to tighten it.  If you are unsure about
a validation rule, make it strict.  If you are unsure about a tag, ask for
help.

## Basic rules for using tags

Everyone who is reviewing APIs and ideally everyone who is writing them should
learn these rules.

### Alpha / beta tags

Some tags are designated "alpha" or "beta" in validation-gen.  If we try to use
such a tag in our APIs, we will get a warning.  To get around this, we must
acknowledge that we are using an alpha or beta tag by adding a `+k8s:alpha` or
`+k8s:beta` tag prefix.  Example:

```
// +k8s:alpha(since: "0.0")=+k8s:theAlphaTagWeWantToUse
```

### Rules for all fields

Every field must be either `+k8s:required` or `+k8s:optional`.  Any field whose
value is the zero value for that type is considered "unspecified".  Required
fields must be specified and optional fields may be unspecified.  This is about
the value of the field and NOT about whether the value was present "on the
wire".  Protobuf generally omits zero values from the wire, but that is not a
requirement.

For example, if (for some reason) we received a protobuf message with an
optional int32 field set to 0, then that field would be considered
"unspecified" even though it was present on the wire.  If that field is marked
as `+k8s:required`, then the message would be rejected.

### Rules for int fields

Most int fields should be validated with either or both of the `+k8s:minimum`
and `+k8s:maximum` tags.  If a field is marked as `+k8s:required`, then that
field may not hold the zero value for that type unless the field also has the
tag `+default=0` (note the different syntax).

In some cases fields will have only one one bound (often the minimum).  An
example of this is a field that can hold any positive integer, in which case
you might see:

```
// +k8s:optional
// +k8s:minimum=1
```

### Rules for bool fields

Think twice about boolean fields - they often end up with a third state,
requiring awkward evolution.  Consider using an enum instead of a boolean.

### Rules for string fields

All string fields must have either a `+k8s:format=<format>` tag or a
`+k8s:maxLength=<length>` tag.  The format tag is preferred when possible.
validation-gen supports a number of formats, and can easily be extended to
support new ones.

If validation-gen does not support the format you need, you can use a
combination of `+k8s:maxLength` and `+k8s:customValidation` (see below) tags.
If a string field does not have an a priori known format, then it must at least
have a `+k8s:maxLength` tag.

### Rules for struct (message) fields

Struct fields are usually just tagged as required or optional (relying on the
fields within the type to be tagged). Sometimes the specific usage site wants
to impose further restrictions on the struct, which can be done with one or
more `+k8s:subfield` tags or a `+k8s:customValidation` tag (see below).

### Rules for repeated fields (lists)

Repeated fields must also be tagged as optional or required.  If a repeated
field is required, then it must have at least one element.  If a repeated field
is optional, then it may have zero elements (we do not distinguish between a
`nil` slice and a slice with 0 elements).

All repeated fields must has a `+k8s:maxItems` tag, and some may need a
`+k8s:minItems` tag.

For lists of messages, each item in the list will be validated as per its
type-attached rules.

#### List types

All repeated fields must specify a `+k8s:listType=` tag.

##### Set-like lists

Some repeated fields, usually but not always primitives (e.g. `string`), must
hold unique values.  This can be designated with the `+k8s:listType=set` tag.
Such a list field will fail validation if it contains duplicate values.

The uniqueness of values may be used in the future to allow patch-like
operations.

##### Map-like lists 

Some repeated message fields hold values which must be unique (within that
list) based on some key field(s) of the message.  This can be designated with
the `+k8s:listType=map` and `+k8s:listMapKey=<field-name>` tags.  Such a field
may have multiple key-fields.  Such a list field will fail validation if it
contains multiple elements with the same key field values.  Key fields must be
required.  Example:

```
message Foo {
    // +k8s:optional
    // +k8s:listType=map
    // +k8s:listMapKey=key1
    // +k8s:listMapKey=key2
    repeated Bar myList = 1;
}

message Bar {
    // +k8s:required
    string key1 = 1;

    // +k8s:required
    string key2 = 2;

    // +k8s:required
    string data = 3;
}
```

The uniqueness of values may be used in the future to allow patch-like
operations.

##### Atomic lists

Lists which are not set-like or map-like are considered "atomic" and are
designated with the `+k8s:listType=atomic` tag.  Atomic lists do not have any
uniqueness constraints.

#### Per-item validation

Many list fields need to impose further validation on each item in the list.
This can be done with one or more `+k8s:eachVal` tags or a
`+k8s:customValidation` tag (see below).  Example:

```
// +k8s:optional
// +k8s:eachVal=+k8s:maxLength=32
repeated string options = 1;
```

All of the rules established in this document for scalar fields also apply to
the items in a repeated field.  For example, all lists of strings must have a
format or a maxLength tag.

### Rules for map fields

Map fields must also be tagged as optional or required.  If a map field is
required, then it must have at least one key-value pair.  If a map field is
optional then it may have zero key-value pairs (we do not distinguish between a
`nil` map and a map with 0 key-value pairs).

All map fields must have a `+k8s:maxProperties` tag, and some may need a
`+k8s:minProperties` tag.

For maps of messages, each value in the map will be validated as per its
type-attached rules.

#### Per-item validation

Many map fields need to impose further validation on each key and/or value in
the map.  This can be done with one or more `+k8s:eachKey` and/or
`+k8s:eachVal` tags or a `+k8s:customValidation` tag (see below).  Example:

```
// +k8s:optional
// +k8s:eachKey=+k8s:maxLength=32
// +k8s:eachVal=+k8s:maxLength=256
map<string, string> options = 1;
```

All of the rules established in this document for scalar fields also apply to
the keys and values in a map field.  For example, all maps with string keys
must have a format or a maxLength tag.

## Updates

Declarative validation is applied to both create and update operations.  Some
fields are truly mutable, in which case they need not specify anything about
updates, but some fields are more restricted.  For example, some fields are
immutable, and some fields may be mutable but only under certain conditions.

### Immutable fields

Immutable fields may be set one time, at creation, and must remain the same
thereafter.  This is specified with the `+k8s:immutable` tag.  Example:

```
// +k8s:optional
// +k8s:immutable
string name = 1;
```

The name field may be specified when the object is created, but it may not be
changed in any update.  Since this field is optional, it may be left
unspecified at creation time, and will not be allowed to be specified in any
update.

### Fine-grained update rules

Some fields are more complicated, and may be mutable in certain ways.  For
example a field may be mutable only if it is being changed from unspecified to
specified, but not the reverse.  This is what the `+k8s:update` tag is for.

The update tag supports the following payload values and may be specified more
than once:

  * NoSet: The field may not be changed from unspecified to specified, but may
    be otherwise modified.
  * NoUnset: The field may not be changed from specified to unspecified, but may
    be otherwise modified.
  * NoModify: The field may not be changed from one value to another, but may be
    set or cleared.
  * NoAddItem: The list field may not have items added to it, but may have
    items removed or modified.
  * NoRemoveItem: The list field may not have items removed from it, but may
    have items added or modified.

## Advanced rules

### Oneof (aka unions)

Frequently we need to represent a set of fields of which exactly one must be
specified.  While protobuf has a `oneof` keyword, it does not mix well with
declarative validation, so we do not use it.  Instead we specify multiple
optional fields which are each tagged as `+k8s:unionMember`.  The generated
validation code will ensure that exactly one of the fields is specified.

This is wire-compatible with protobuf oneofs.

Because protobuf elides zero-value primitives from the wire, unions must not
include primitive fields where the zero-value is valid unless those fields use
the optional `optional` keyword in protobuf, which converts them into a pointer
type.  For example, DV cannot tell which of these options was intended when the
value 0 is allowed:

```
message Foo {
    // +k8s:optional
    // +k8s:minimum=0
    // +k8s:unionMember
    string opt1 = 1;

    // +k8s:optional
    // +k8s:minimum=0
    // +k8s:unionMember
    string opt2 = 2;
}
```

But this is unambiguous:

```
message Bar {
    // +k8s:optional
    // +k8s:minimum=0
    // +k8s:unionMember
    optional string opt1 = 1;

    // +k8s:optional
    // +k8s:minimum=0
    // +k8s:unionMember
    optional string opt2 = 2;
}
```

### Subfields

Sometimes a struct type's internal validation rules are looser than we want for
a given usage site.  This is usually related to types which are used in many
places.  In those cases, we can use the `+k8s:subfield` tag to impose
additional validation on the fields of the struct.  Example:

```
message ObjectRef {
  // +k8s:optional
  string atespace = 1;

  // +k8s:required
  string name = 2;
}
```

In this case, an ObjectRef might hold a reference to an object which is
atespaced or which is not.  If we want to use an ObjectRef in a context where
atespace must be specified, we can do this:

```
// +k8s:required
// +k8s:subfield(atespace)=+k8s:required
ObjectRef actor = 1;
```

Similarly, if we want to use an ObjectRef in a context where atespace must NOT
be specified, we can do this:

```
// +k8s:required
// +k8s:subfield(atespace)=+k8s:forbidden
ObjectRef worker = 1;
```

Subfields may not be nested, but that will come in a future release.  If you
need deeper subfield validation, you can use `+k8s:customValidation` (see below).

### Custom validation

Whenever the declarative validation tags are insufficient, you can write a
custom validation function.  This is done with the `+k8s:customValidation` tag.
The generated code will call a particularly named function, which you must
implement.  The function will be passed the field value and must return a
`field.ErrorList` if the value is invalid.  For example, given:

```
message Foo {
  // +k8s:optional
  // +k8s:customValidation
  string bar = 1;
}
```

validation-gen will look for a function with the signature:

```
func ValidateCustom_Foo_Bar(context.Context, operation.Operation,
    fldPath *field.Path, val, oldVal <type>) field.ErrorList {
```

If the `+k8s:customValidation` tag is attached to a struct field, then the
function will receive a pointer to the struct type.  If the tag is attached to
a scalar field, then the function will receive a pointer to the scalar type.
If the tag is attached to a repeated or map field, then the function will
receive a slice of the element type or the defined map type.

All uses of `+k8s:customValidation` should be commented in the proto file to
indicate what validation is being performed.

### Opaque types

Sometimes we want to cancel or replace the validation rules which are attached
to a type. This is done with the `+k8s:opaqueType` tag.  Example:

```
message Foo {
  // +k8s:required
  string name = 1;
}

message Bar {
  // +k8s:required
  // +k8s:opaqueType
  Foo foo = 1;
}
```

In this case, the `Bar.foo.name` field will NOT be validated as required,
because the `opaqueType` tag cancels the validation rules of the `Foo` type.
This is a big hammer, use it with care.
