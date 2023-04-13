#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip install mysql-connector-python')


# In[3]:


get_ipython().system('pip install pandas')


# In[4]:


#Load required libraries
import mysql.connector
import pandas as pd


# In[5]:


connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="password")
# creating a cursor object
cursorObject = connection.cursor()


# In[6]:


## Lets make a connection to Mysql server and create a database named 'E_Commerce'
connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="password")
 
## creating a cursor object
cursorObject = connection.cursor()
 
## creating database
cursorObject.execute("CREATE DATABASE E_Commer")

## closing the connection after creating a database('e_commerce')
connection.close()


# In[7]:


## Again connect to the Mysql server and while connecting

connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="password",
                                     database = 'E_Commer')
 
## creating a cursor object
cursorObject = connection.cursor()


# In[8]:


## Solution for Q1: Creating tables 
table_creation_query = """create table supplier(SUPP_ID int primary key, SUPP_NAME varchar(50), SUPP_CITY varchar(50), SUPP_PHONE varchar(10));
                          
                          create table customer(CUS_ID INT NOT NULL, CUS_NAME VARCHAR(20) NULL DEFAULT NULL, CUS_PHONE VARCHAR(10), CUS_CITY varchar(30) ,CUS_GENDER CHAR,PRIMARY KEY (CUS_ID));
                          
                          create table category(CAT_ID INT NOT NULL, CAT_NAME VARCHAR(20) NULL DEFAULT NULL,PRIMARY KEY (CAT_ID));
                          
                          create table product(PRO_ID INT NOT NULL, PRO_NAME VARCHAR(20) NULL DEFAULT NULL, PRO_DESC VARCHAR(60) NULL DEFAULT NULL, CAT_ID INT NOT NULL,PRIMARY KEY (PRO_ID),FOREIGN KEY (CAT_ID) REFERENCES CATEGORY (CAT_ID));
                          
                          create table product_details(PROD_ID INT NOT NULL, PRO_ID INT NOT NULL, SUPP_ID INT NOT NULL, PROD_PRICE INT NOT NULL,
                          PRIMARY KEY (PROD_ID),FOREIGN KEY (PRO_ID) REFERENCES PRODUCT (PRO_ID), FOREIGN KEY (SUPP_ID) REFERENCES SUPPLIER(SUPP_ID));
                          
                          create table orders(ORD_ID INT NOT NULL, ORD_AMOUNT INT NOT NULL, ORD_DATE DATE, CUS_ID INT NOT NULL, PROD_ID INT NOT NULL,PRIMARY KEY (ORD_ID),FOREIGN KEY (CUS_ID) REFERENCES CUSTOMER(CUS_ID),FOREIGN KEY (PROD_ID) REFERENCES PRODUCT_DETAILS(PROD_ID));
                          
                       create table rating(RAT_ID INT NOT NULL, 
                  CUS_ID INT NOT NULL, SUPP_ID INT NOT NULL, 
                  RAT_RATSTARS INT NOT NULL,PRIMARY KEY (RAT_ID),FOREIGN KEY (SUPP_ID) REFERENCES SUPPLIER (SUPP_ID),FOREIGN KEY (CUS_ID) REFERENCES CUSTOMER(CUS_ID)); """
# Executing the query
cursorObject.execute(table_creation_query)


# In[9]:


## Solution for Q2: Inserting data to the tables

## inserting data into supplier

insert_query="insert into supplier(SUPP_ID ,SUPP_NAME,SUPP_CITY ,SUPP_PHONE ) values (%s,%s,%s,%s)"
val =[(1,"Rajesh Retails","Delhi","1234567890"),
      (2,"Appario Ltd.","Mumbai","2589631470"),
      (3,"Knome products","Bangalore","9785462315"),
      (4,"Bansal Retails", "Kochi", "8975463285"),
      (5,"Mittal Ltd."," Lucknow","7898456532") ]
cursorObject.executemany(insert_query,val)
connection.commit()


# In[10]:


## inserting into customers

insert_query="insert into customer(CUS_ID,CUS_NAME,CUS_PHONE,CUS_CITY,CUS_GENDER) values (%s,%s,%s,%s,%s)"
val = [(1,"AAKASH","9999999999","DELHI","M "),
        (2,"AMAN","9785463215","NOIDA","M"),
        (3,"NEHA","9999999998","MUMBAI","F"),
        (4,"MEGHA","9994562399","KOLKATA","F"),
        (5,"PULKIT","7895999999","LUCKNOW","M")]
cursorObject.executemany(insert_query,val)
connection.commit()


# In[13]:


##inserting into category

insert_query = "insert into category(CAT_ID,CAT_NAME) values (%s,%s)"
val= [(1,"BOOKS"),
      (2,"GAMES"),
      (3,"GROCERIES"),
      (4,"ELECTRONICS"),
      (5,"CLOTHES")]
cursorObject.executemany(insert_query,val)
connection.commit()


# In[14]:


## inserting into product

insert_query = "insert into product(PRO_ID,PRO_NAME,PRO_DESC,CAT_ID ) values (%s,%s,%s,%s)"
val = [(1,"GTA V","DFJDJFDJFDJFDJFJF",2),
        (2,"TSHIRT","DFDFJDFJDKFD",5), 
        (3,"ROG LAPTOP","DFNTTNTNTERND",4),
        (4,"OATS","REURENTBTOTH",3),
        (5,"HARRY POTTER","NBEMCTHTJTH",1)]
cursorObject.executemany(insert_query,val)
connection.commit()


# In[15]:


## inserting into product details

insert_query = "insert into product_details(PROD_ID,PRO_ID,SUPP_ID,PROD_PRICE) values (%s,%s,%s,%s)"
val = [(1,1,2,1500),
      (2,3,5,30000),
      (3,5,1,3000),
      (4,2,3,2500),
      (5,4,1,1000),]
cursorObject.executemany(insert_query,val)
connection.commit()


# In[16]:


## inserting into orders

insert_query = "insert into orders(ORD_ID,ORD_AMOUNT,ORD_DATE,CUS_ID,PROD_ID) values (%s,%s,%s,%s,%s)"
val = [(20,1500,'2021-10-12',3,5),
       (25,30500,'2021-09-16',5,2),
      (26,2000,'2021-10-05',1,1),
      (30,3500,'2021-08-16',4,3),
      (50,2000,'2021-10-06',2,1),]
cursorObject.executemany(insert_query,val)
connection.commit()


# In[17]:


## inserting into rating

insert_query = "insert into rating(RAT_ID,CUS_ID,SUPP_ID,RAT_RATSTARS) values (%s,%s,%s,%s)"
val = [(1,2,2,4),
      (2,3,4,3),
      (3,5,1,5),
      (4,1,3,2),
      (5,4,5,4)]
cursorObject.executemany(insert_query,val)
connection.commit()


# In[21]:


Query3= """select customer.cus_gender,count(customer.cus_gender) as count from customer inner join `orders`
            on customer.cus_id=`orders`.cus_id where `orders`.ord_amount>=3000 group by customer.cus_gender;"""

cursorObject.execute(Query3)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['CUS_GENDER','COUNT'])
output_df


# In[26]:


Query4= """select `orders`.*,product.pro_name from `orders`,product_details, product 
           where `orders`.cus_id = 2 and `orders`.prod_id=product_details.prod_id 
           and product_details.pro_id=product.pro_id"""

cursorObject.execute(Query4)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['ORD_ID','ORD_AMOUNT','ORD_DATE','ORD_ID','PROD_ID','PRO_NAME'])
output_df


# In[11]:


Query5= """select supplier.* from supplier,product_details where supplier.supp_id in
        (select product_details.supp_id from product_details group by product_details.supp_id
        having count(product_details.supp_id)>1) group by supplier.supp_id;"""

cursorObject.execute(Query5)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['SUPP_ID','SUPP_NAME','SUPP_CITY','SUPP_PHONE'])
output_df


# In[12]:


Query6= """select category.* from `orders` inner join product_details on `orders`.prod_id=product_details.prod_id
           inner join product on product.pro_id=product_details.pro_id inner join category on category.cat_id=product.cat_id 
           having min(`orders`.ord_amount);"""

cursorObject.execute(Query6)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['CAT_ID','CAT_NAME'])
output_df


# In[15]:


Query7= """select product.pro_id,product.pro_name 
           from `orders` inner join product_details on product_details.prod_id=`orders`.prod_id
           inner join product on product.pro_id=product_details.pro_id where `orders`.ord_date>"2021-10-05"; """

cursorObject.execute(Query7)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['PRO_ID','PRO_NAME'])
output_df


# In[16]:


Query8 = """select supplier.supp_id,supplier.supp_name,customer.cus_name,rating.rat_ratstars from rating
            inner join supplier on rating.supp_id=supplier.supp_id inner join customer on rating.cus_id=customer.cus_id 
            order by rating.rat_ratstars desc limit 3;"""


cursorObject.execute(Query8)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['SUPP_ID','SUPP_NAME','CUS_NAME','RAT_RATSTARS'])
output_df


# In[18]:


Query9 = """ select customer.cus_name, customer.cus_gender from customer where customer.cus_name 
              like '%A' or customer.cus_name like 'A%'"""


cursorObject.execute(Query9)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['CUS_NAME','CUS_GENDER'])
output_df


# In[23]:


Query10= """select sum(`orders`.ord_amount) as Amount
            from `orders` inner join customer on `orders`.cus_id=customer.cus_id where customer.cus_gender='M';"""

cursorObject.execute(Query10)
output= cursorObject.fetchall()

print("amount is",output);


# In[24]:


Query11= """select * from customer left outer join `orders` on customer.cus_id=`orders`.cus_id;"""


cursorObject.execute(Query11)
output= cursorObject.fetchall()

#output in pandas dataframe
output_df=pd.DataFrame(output,columns=['CUS_ID','CUS_NAME','CUS_PHONE','CUS_CITY','CUS_GENDER','ORD_ID','ORD_AMOUNT'
                                      ,'ORD_DATE','CUS_ID','PROD_ID'])
output_df


# In[ ]:




