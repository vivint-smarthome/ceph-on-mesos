.PHONY:
.SECONDARY:

README.md: project/version.properties
	bin/update-version
