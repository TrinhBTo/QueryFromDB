# QueryFromDB Project


This project involves creating a website that demonstrates the integration of multiple cloud services, including object storage and NoSQL databases. The project is a functional web application hosted in Microsoft Azure.

## Overview

The goal of this project is to build a web application that uses various cloud services to interact with data. The application allows users to load data from a specified URL into cloud storage (Amazon S3) and then parse and load the data into a NoSQL database (DynamoDB). Users can also query the data based on exact matches of first and last names.

- Clicking the "Load Data" button fetches data from the specified URL and stores it in a NoSQL database.
- The "Clear Data" button removes data from the cloud storage and empties or removes the NoSQL database.
- Users can input first and last names and query the data for exact matches.
- The website allows for repeated data loading without erasing existing records in the NoSQL database.

## DB content (2023)

* **Last - First - Information**
* Lee  Brent myid=123  phone=123-456-7890  age=35
* Brent Charles myid=345 phone=234-567-8901 age=20
* Lee Charles age=22
* Dylan Robert  myid=65764 phone=4528769876 office=uw1
* Karenina Anna age=140 myid=3456
* Conway Daniel office=home  age=23
* Kitty Meow office=sandbox age=2 cuteness=overload
