# Agent Substrate label registry

This document lists label keys Agent Substrate claims as API or integration
contract. Provider implementations may use other labels internally, but only
labels listed here should be treated as portable substrate behavior.

## Pod labels

### `pod.ate.dev/is-worker`

Value: `"true"`

Marks a Kubernetes Pod as an Agent Substrate Worker. A Kubernetes worker syncer
may watch Pods with this label and create/update/delete corresponding Worker
records in ateapi.

