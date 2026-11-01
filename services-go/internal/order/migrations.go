package order

import "io/fs"

// Migrations returns the order database migrations.
func Migrations() fs.FS {
	migrations, _ := fs.Sub(embeddedMigrations, "migrations")
	return migrations
}
