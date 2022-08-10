#!/usr/bin/bash

echo "updating the packages .."
sudo yum update -y
echo "package update completed"
echo "................"
echo "................."
echo "Installing docker on AWS Linux"
sudo amazon-linux-extras install docker
echo "Installing docker on AWS Linux completed"
echo "................."
echo "Starting docker..."
sudo service docker start
echo "Docker Started" 
echo "................."
echo "Enabling docker start after every reboot" 
sudo systemctl enable docker
echo "Removing sudo usage and enable to run docker commands directly"
sudo usermod -a -G docker ec2-user	    