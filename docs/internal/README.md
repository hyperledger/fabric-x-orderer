
# Development Environment

More information about the local development environment.

## ARMA imports fabricx-config

ARMA imports the `fabricx-config`, which is a private repository that was created to collect all configuration and generation tools from Fabric internals.
To make import of this repository successful, the following steps were done:

**For Travis:**
1. Create an access token in GitHub:   
    Go to GitHub -> Press on Settings -> Developer Settings -> Personal access tokens -> Tokens (classic) -> Generate new token -> Generate new token (classic).
    Choose a name, an expiration date (for convenience, you can choose no expiration) and select the repo option. Then, generate the token.
    Don’t forget to copy the token.
2. Configure .travis.yml:  
    Edit the zshrc
    `vim ~/.zshrc`  
    the content of the file to match the following:
    ```
   export GO111MODULE=on
    export GOPRIVATE=github.ibm.com
    git config --global url."https://${GITHUB_USER}:${GITHUB_TOKEN}@github.ibm.com".insteadOf "https://github.ibm.com"
   ``` 
3. Update CI credentials:   
   Go to Travis -> More options -> Settings -> Environment Variables  
   Add the `GITHUB_USER` with IBM GitHub username and `GITHUB_TOKEN` with the access token acquired in the step #1.

NOTE: A single completion of the steps by any group member is sufficient. Next PR's should pass as usual.


**For Local Setup:**  
If you're the first one to import the private repository you should do the following:
1. Configure Go environment variable to inform Go which module paths should be considered private  
   `export GOPRIVATE=github.ibm.com`
2. Add or edit the global .gitconfig:  
   `git config --global user.name "<Your Name>"`  
   `git config --global user.email <IBM Email>`  
   `git config --global url.ssh://git@github.ibm.com/.insteadOf https://github.ibm.com/`  
    Running `git config --global -l`, you should get:
   ```
   user.name=Your Name
   user.email=IBM Email
   url.ssh://git@github.ibm.com/.insteadof=https://github.ibm.com/
   ```
3. If you get a 'write access to repository not granted' message, probably you don't have permissions. 
   To resolve this, reach out to the authorized members and ask them to grant you access.


## Login to IBM Cloud

Note: to login to IBM cloud:

  `ibmcloud login  --sso`
  `ibmcloud target -g zrl-dec-trust-identity`
  `ibmcloud cr login`

