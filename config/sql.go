package config

type (
	// SQL is the configuration for connecting to a SQL backed datastore
	SQL struct {
		// User is the username to be used for the conn
		// If useMultipleDatabases, must be empty and provide it via multipleDatabasesConfig instead
		User string `yaml:"user"`
		// Password is the password corresponding to the user name
		// If useMultipleDatabases, must be empty and provide it via multipleDatabasesConfig instead
		Password string `yaml:"password"`
		// DatabaseName is the name of SQL database to connect to
		// If useMultipleDatabases, must be empty and provide it via multipleDatabasesConfig instead
		// Required if not useMultipleDatabases
		DatabaseName string `yaml:"databaseName"`
		// ConnectAddr is the remote addr of the database
		// If useMultipleDatabases, must be empty and provide it via multipleDatabasesConfig instead
		// Required if not useMultipleDatabases
		ConnectAddr string `yaml:"connectAddr"`
		// DBExtensionName is the name of the extension
		DBExtensionName string `yaml:"dbExtensionName"`
	}
)
