package extensions

const (
	// CLIOptEndpoint is the cli option for endpoint
	CLIOptEndpoint = "endpoint"
	// CLIOptPort is the cli option for port
	CLIOptPort = "port"
	// CLIOptUser is the cli option for user
	CLIOptUser = "user"
	// CLIOptPassword is the cli option for password
	CLIOptPassword = "password"
	CLIOptDatabase = "database"

	// CLIFlagEndpoint is the cli flag for endpoint
	CLIFlagEndpoint = CLIOptEndpoint + ", ep"
	// CLIFlagPort is the cli flag for port
	CLIFlagPort = CLIOptPort + ", p"
	// CLIFlagUser is the cli flag for user
	CLIFlagUser = CLIOptUser + ", u"
	// CLIFlagPassword is the cli flag for password
	CLIFlagPassword = CLIOptPassword + ", pw"
	CLIFlagDatabase = CLIOptDatabase + ", db"
)
