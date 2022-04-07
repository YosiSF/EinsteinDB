#!/bin/bash
echo "Welcome to EinsteinDB"


echo "Installing Docker..."; sleep 2s;  #sleep is used for delay in 
execution of the script. It can be removed if required.
sudo apt-get update -y && sudo apt-get install docker.io -y > /dev/null 
2>&1 & wait $! || { echo 'Docker Installation Failed' ; exit 1; }   #This 
will check whether the installation was successful or not and proceed 
accordingly, else it will throw an error message and exit with status code 
1(failure) which can be checked by running echo $? after executing this 
script on terminal. This command also redirects all output to null so that 
no messages are displayed while installing docker except for errors (if 
any). The ampersand at end makes sure that next line executes only when 
current one finishes its execution successfully without throwing any error 
codes other than 0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 		                                                      
#This will check whether the installation was successful or not and 
proceed accordingly, else it will throw an error message and exit with 
status code 1(failure) which can be checked by running echo $? after 
executing this script on terminal. This command also redirects all output 
to null so that no messages are displayed while installing docker except 
for errors (if any). The ampersand at end makes sure that next line 
executes only when current one finishes its execution successfully without 
throwing any error codes other than 0(success).
	echo "Docker Installation Successful"; sleep 2s;  #sleep is used 
for delay in execution of the script. It can be removed if required.

	sudo systemctl start docker > /dev/null 2>&1 & wait $! || { echo 
'Failed to Start Docker' ; exit 1; }   #This will check whether starting 
of service was successful or not and proceed accordingly, else it will 
throw an error message and exit with status code 1(failure) which can be 
checked by running echo $? after executing this script on terminal. This 
command also redirects all output to null so that no messages are 
displayed while starting service except for errors (if any). The ampersand 
at end makes sure that next line executes only when current one finishes 
its execution successfully without throwing any error codes other than 
0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        sudo systemctl enable docker > /dev/null 2>&1 & wait $! || { echo 
'Failed to Enable Docker Service' ; exit 1; }   #This will check whether 
enabling of service was successful or not and proceed accordingly, else it 
will throw an error message and exit with status code 1(failure) which can 
be checked by running echo $? after executing this script on terminal. 
This command also redirects all output to null so that no messages are 
displayed while enabling service except for errors (if any). The ampersand 
at end makes sure that next line executes only when current one finishes 
its execution successfully without throwing any error codes other than 
0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        echo "Docker Service Started and Enabled"; sleep 2s;  #sleep is 
used for delay in execution of the script. It can be removed if required.
	sudo usermod -aG docker $USER > /dev/null 2>&1 & wait $! || { echo 
'Failed to Add User to Docker Group' ; exit 1; }   #This will check 
whether adding user was successful or not and proceed accordingly, else it 
will throw an error message and exit with status code 1(failure) which can 
be checked by running echo $? after executing this script on terminal. 
This command also redirects all output to null so that no messages are 
displayed while adding user except for errors (if any). The ampersand at 
end makes sure that next line executes only when current one finishes its 
execution successfully without throwing any error codes other than 
0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        sudo systemctl restart docker > /dev/null 2>&1 & wait $! || { echo 
'Failed to Restart Docker Service' ; exit 1; }   #This will check whether 
restarting service was successful or not and proceed accordingly, else it 
will throw an error message and exit with status code 1(failure) which can 
be checked by running echo $? after executing this script on terminal. 
This command also redirects all output to null so that no messages are 
displayed while restarting service except for errors (if any). The 
ampersand at end makes sure that next line executes only when current one 
finishes its execution successfully without throwing any error codes other 
than 0(success).

	echo "User Added Successfully To Docker Group"; sleep 2s;  #sleep 
is used for delay in execution of the script. It can be removed if 
required.

	fi
	fi
	fi

 echo "Installing Docker Compose..."; sleep 2s;  #sleep is used for delay 
in execution of the script. It can be removed if required.
        sudo curl -L 
https://github.com/docker/compose/releases/download/1.25.0-rc2/docker-compose-`uname 
-s`-`uname -m` > /usr/local/bin/docker-compose > /dev/null 2>&1 & wait $! 
|| { echo 'Failed to Download Docker Compose' ; exit 1; }   #This will 
check whether downloading was successful or not and proceed accordingly, 
else it will throw an error message and exit with status code 1(failure) 
which can be checked by running echo $? after executing this script on 
terminal. This command also redirects all output to null so that no 
messages are displayed while downloading except for errors (if any). The 
ampersand at end makes sure that next line executes only when current one 
finishes its execution successfully without throwing any error codes other 
than 0(success).

        if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        sudo chmod +x /usr/local/bin/docker-compose > /dev/null 2>&1 & 
wait $! || { echo 'Failed to Change Permissions' ; exit 1; }   #This will 
check whether changing permissions was successful or not and proceed 
accordingly, else it will throw an error message and exit with status code 
1(failure) which can be checked by running echo $? after executing this 
script on terminal. This command also redirects all output to null so that 
no messages are displayed while changing permissions except for errors (if 
any). The ampersand at end makes sure that next line executes only


        if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        echo "Docker Compose Installation Successful"; sleep 2s;  #sleep 
is used for delay in execution of the script. It can be removed if 
required.
	echo "Installing Jekyll..."; sleep 2s;  #sleep is used for delay 
in execution of the script. It can be removed if required.
	sudo apt-get install ruby-full build-essential zlib1g-dev > 
/dev/null 2>&1 & wait $! || { echo 'Failed to Install Ruby' ; exit 1; }   
#This will check whether installing was successful or not and proceed 
accordingly, else it will throw an error message and exit with status code 
1(failure) which can be checked by running echo $? after executing this 
script on terminal. This command also redirects all output to null so that 
no messages are displayed while installing except for errors (if any). The 
ampersand at end makes sure that next line executes only when current one 
finishes its execution successfully without throwing any error codes other 
than 0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        sudo gem install jekyll bundler > /dev/null 2>&1 & wait $! || { 
echo 'Failed to Install Jekyll' ; exit 1; }   #This will check whether 
installing was successful or not and proceed accordingly, else it will 
throw an error message and exit with status code 1(failure) which can be 
checked by running echo $? after executing this script on terminal. This 
command also redirects all output to null so that no messages are 
displayed while installing except for errors (if any). The ampersand at 
end makes sure that next line executes only when current one finishes its 
execution successfully without throwing any error codes other than 
0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        echo "Jekyll Installation Successful"; sleep 2s;  #sleep is used 
for delay in execution of the script. It can be removed if required.
	echo "Installing MySQL..."; sleep 2s;  #sleep is used for delay in 
execution of the script. It can be removed if required.
	sudo apt-get install mysql-server > /dev/null 2>&1 & wait $! || { 
echo 'Failed to Install MySQL' ; exit 1; }   #This will check whether 
installing was successful or not and proceed accordingly, else it will 
throw an error message and exit with status code 1(failure) which can be 
checked by running echo $? after executing this script on terminal. This 
command also redirects all output to null so that no messages are 
displayed while installing except for errors (if any). The ampersand at 
end makes sure that next line executes only when current one finishes its 
execution successfully without throwing any error codes other than 
0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        sudo systemctl start mysql > /dev/null 2>&1 & wait $! || { echo 
'Failed to Start MySQL Service' ; exit 1; }   #This will check whether 
starting service was successful or not and proceed accordingly, else it 
will throw an error message and exit with status code 1(failure) which can 
be checked by running echo $? after executing this script on terminal. 
This command also redirects all output to null so that no messages are 
displayed while starting service except for errors (if any). The ampersand 
at end makes sure that next line executes only when current one finishes 
its execution successfully without throwing any error codes other than 
0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 


	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        sudo systemctl enable mysql > /dev/null 2>&1 & wait $! || { echo 
'Failed to Enable MySQL Service' ; exit 1; }   #This will check whether 
enabling service was successful or not and proceed accordingly, else it 
will throw an error message and exit with status code 1(failure) which can 
be checked by running echo $? after executing this script on terminal. 
This command also redirects all output to null so that no messages are 
displayed while enabling service except for errors (if any). The ampersand 
at end makes sure that next line executes only when current one finishes 
its execution successfully without throwing any error codes other than 
0(success).

	if [ "$?" = "0" ]; then    #If previous command executed 
successfully, then execute following commands: 
        echo "MySQL Service Started and Enabled"; sleep 2s;  #sleep is 
used for delay in execution of the script. It can be removed if required.
	sudo mysql_secure_installation > /dev/null 2>&1 & wait $! || { 
echo 'Failed to Secure MySQL Installation' ; exit 1; }   #This will check 
whether securing installation was successful or not and proceed 
accordingly, else it will throw an error message and exit with status code 
1(failure) which can be checked by running echo $? after executing this 
script on terminal. This command also redirects all output to null so that 
no messages are displayed while securing installation except for errors 
(if any). The ampersand at end makes sure that next line executes only 
when current one finishes its execution successfully without throwing any 
error codes other than 0(success).

	echo "MySQL Installation Successful"; sleep 2s;  #sleep is used 
for delay in execution of the script. It can be removed if required
