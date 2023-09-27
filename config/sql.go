package config

type (
	// SQL is the configuration for connecting to a SQL backed datastore
	SQL struct {
		// User is the username to be used for connecting to database
		User string `yaml:"user"`
		// Password is the password corresponding to the username
		Password string `yaml:"password"`
		// DatabaseName is the name of SQL database to connect to
		DatabaseName string `yaml:"databaseName"`
		// ConnectAddr is the remote addr of the database
		ConnectAddr string `yaml:"connectAddr"`
		// DBExtensionName is the name of the extension
		DBExtensionName string `yaml:"dbExtensionName"`
	}
)
