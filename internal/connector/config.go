package connector

import "fmt"

type config struct {
	url  string
	user string
	pass string
	ns   string

	// either user/pass or token needs to be set
	token string
}

func (c *config) validate() error {
	var missingFields []string

	if c.url == "" {
		missingFields = append(missingFields, "url")
	}
	if c.ns == "" {
		missingFields = append(missingFields, "ns")
	}

	if c.token == "" {
		if c.user == "" || c.pass == "" {
			return fmt.Errorf("either token or user/pass needs to be set")
		}
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("missing required fields: %v", missingFields)
	}

	return nil
}

// parseConfig parses the Fivetran connector configuration and returns a config instance
func (s *Server) parseConfig(configuration map[string]string) (config, error) {
	cfg := config{
		url:  configuration["url"],
		ns:   configuration["ns"],
		user: configuration["user"],
		pass: configuration["pass"],
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}
