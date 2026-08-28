// Copyright © 2026 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbsql

import "errors"

// SQLSTATE class 23 (integrity constraint violation) codes
const (
	SQLStateForeignKeyViolation = "23503"
	SQLStateUniqueViolation     = "23505"
)

// ConstraintViolation describes an integrity constraint violation reported by the database
type ConstraintViolation struct {
	// SQLState is the SQLSTATE code identifying the kind of violation, e.g. SQLStateUniqueViolation
	SQLState string
	// Constraint is the name of the violated constraint, or empty if the driver does not expose it
	Constraint string
}

// sqlStateError is implemented by driver errors that expose the SQLSTATE code - notably *pq.Error (lib/pq)
// and *pgconn.PgError (pgx). Duck-typing it here avoids a dependency on any particular driver.
type sqlStateError interface {
	SQLState() string
}

// SQLStateConstraintViolationClassifier is the default ConstraintViolationClassifier, matching any error in the
// chain that reports a SQLSTATE unique or foreign key violation. It cannot determine the constraint name.
func SQLStateConstraintViolationClassifier(err error) *ConstraintViolation {
	var sqlErr sqlStateError
	if errors.As(err, &sqlErr) {
		switch code := sqlErr.SQLState(); code {
		case SQLStateUniqueViolation, SQLStateForeignKeyViolation:
			return &ConstraintViolation{SQLState: code}
		}
	}
	return nil
}

// IsUniqueViolation reports whether err was caused by a unique constraint violation. The error may be wrapped -
// for example the MsgDBInsertFailed error returned from Insert/InsertTx wraps the underlying driver error - and
// the whole chain is inspected.
//
// This lets callers rely on a UNIQUE index in the database to enforce uniqueness, and map the resulting error.
//
// If one or more constraint names are supplied, only a violation of one of those constraints is a match.
// Constraint names are only available when the provider supplies a ConstraintViolationClassifier that extracts
// them; if the name cannot be determined the result is false, so callers never mistake an unrelated constraint
// for the one they asked about.
func (s *Database) IsUniqueViolation(err error, constraints ...string) bool {
	return s.isConstraintViolation(err, SQLStateUniqueViolation, constraints)
}

// IsForeignKeyViolation reports whether err was caused by a foreign key constraint violation - an insert or
// update referencing a row that does not exist, or a delete of a row that is still referenced. Wrapping and
// constraint name matching behave as for IsUniqueViolation.
func (s *Database) IsForeignKeyViolation(err error, constraints ...string) bool {
	return s.isConstraintViolation(err, SQLStateForeignKeyViolation, constraints)
}

func (s *Database) isConstraintViolation(err error, sqlState string, constraints []string) bool {
	if err == nil {
		return false
	}
	classify := s.features.ConstraintViolationClassifier
	if classify == nil {
		classify = SQLStateConstraintViolationClassifier
	}
	violation := classify(err)
	if violation == nil || violation.SQLState != sqlState {
		return false
	}
	if len(constraints) == 0 {
		return true
	}
	if violation.Constraint == "" {
		return false
	}
	for _, c := range constraints {
		if c == violation.Constraint {
			return true
		}
	}
	return false
}
