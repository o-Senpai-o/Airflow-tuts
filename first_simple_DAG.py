from airflow import DAG

from airflow.operators.bash_operator import BashOperator

from random import seed, random



# to create a DAG object there are two methods
# 1) using variable
# 2) using Context manager


dag = DAG()

# or 

with DAG() as dag:

# there are certain arguments that needs to be given while creating DAGs
# dag_id :  this is essential because we will access any particular DAG using its ID and airflow will store DAGs using dag_id itself
# scheduke_intervals : when do you want a particular DAG to run  eg @Daily

with DAG("core_concepts", schedule_interval = "@daily", catchup = False) as dag:


#* Tasks:
# task are indivisual work that needs to be done in a sequential manner
# lets first understand how tasks are created and later we will see how we can create relationships between them

# tasks are created using Operators
# there are 3 types of Operators
# 1) Action operator    --> defined to perform operations
# 2) Transfer Operator  --> defined to transfer data
# 3) Sensor Operator    --> defined to sense a criteria to happen and thn trigger something maybe a task


# one famous Operator is Bash_operator which is used to run Bash commands
bash_task = BashOperator(task_id = 'bash_command', bash_command="echo #TODAY", env={"TODAY" : "2023-05-08"})

# other most famous operator is  python operator
# this will run the python function specified in the arguments, along with function nae we have to gve the inputs to function 
# as arguments in Python operator

def print_random_number(number):
    seed(number)
    print(random(seed))

python_task = PythonOperator(task_id = 'print_function', python_callable = print_random_number, op_args = 567)



#* now we need to create dependencies betwween them and there are different ways for this
#* using bitwise right  operation1 >> operation2  meaning first complete operation1 then start operation2
#* is there are multiple tasks then use chain  

bash_task >> python_task

bash_task.set_downstream(python_task)

#? chain(op1, op2, op3, op4)

cross_downstream([op1, op2], [op3, op4])

#meaning
[op1, op2] >> op3
[op1, op2] >> op4
