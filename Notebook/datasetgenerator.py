import json 
import random 
import os 
from datetime import datetime, timedelta


output_dir="./mock.data"
Num_Booking=50
Num_Customer=20
DATE = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")


os.makedirs(output_dir,exist_ok=True)


def generate_customer(n=20):
    customers=[]
    for i in range(1,n+1):
        customer_id=f'C{i:03d}'
        customers.append({
            "customer_id":customer_id,
            "name":f'Customer{i}',
            "email":f'customer{i}@example.com',
            "phone_number": f'{random.randint(6000000000,9999999999)}',
            "sign_up_date":(datetime.now()-timedelta(days=random.randint(10,1000))).strftime("%Y-%m-%d"),
            "status": random.choice(["active", "inactive"])
        })
    return customers

def generate_bookings(customers, n=50):
    bookings=[]
    for i in range(1,n+1):
        booking_id=f"B{i:03d}"
        customer=random.choice(customers)
        car_id=f"CAR{random.randint(100,999)}"

        booking_date=datetime.now()-timedelta(days=random.randint(0,30))
        start_time=booking_date + timedelta(hours=random.randint(8,12))
        end_time=start_time + timedelta(hours=random.randint(2,8))

        bookings.append({
            'booking_id':booking_id,
            'customer_id':customer['customer_id'],
            'car_id': car_id,
            "booking_date": booking_date.strftime("%y-%m-%d"),
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_amount": round(random.uniform(50, 500), 2),
            "status": random.choice(["completed", "cancelled", "ongoing"])
        })
    return bookings



if __name__ == "__main__":
    customer=generate_customer(Num_Customer)
    bookings=generate_bookings(customer,Num_Booking)


    #file names
    customer_file=os.path.join(output_dir,f"zoom_car_customers_{DATE}.json")
    booking_file=os.path.join(output_dir,f'zoom_car_bookings_{DATE}.json')


    with open(customer_file,"w") as f:
        json.dump(customer,f,indent=4)

    with open(booking_file,"w") as f:
        json.dump(bookings,f,indent=4)

    
    print(f"Generated:{customer_file} and {booking_file}")



    

    

