## Insight-DE-project
project ideas


# Idea 1 
### Concept 
Some abstrasct Internet Service which is hosted on EC2. Has users in diffenrent regions.
This service takes huge amounts of stream data. Examples:
- self driving cars
- sensors (fitbit, health devices)
- game data
- social media (tweets)

Company wants to capture streaming metrics in real time and run intensive analytics  to be able to present results on dashboard immidiately. 
Company's goal for the rachitecture to have stable and scalable architecture to process big amounts of data and guarantee low latency responce for customers

### Biz goals 
1) Efficienct use of the cloud resources
2) Low latency responce to all customers
3) Fault tolerancy 24/7 uptime

### Tech requirements 
1) Dynamically scale up or down based on activity of a service 
2) Implement loadbalancing layer
2) Connect to Amazon RDS/Postgres service to manage user profiles and state
3) Store service activity in a Timestream service for future analysis
4) As the system scales, ensure that data is not lost due to processing backlogs
5) Run security enhanced Linux 

***

# Idea 2 
### Concept 

