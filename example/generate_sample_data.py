import random
import string
import random
import string

def generate_random_amount():
    random_number = random.uniform(0, 999999.99)
    amount_str = "{:.2f}".format(random_number)
    fixed_length_amount = amount_str.zfill(11)

    return fixed_length_amount

def generate_random_email():
    username_length = 5
    domain_length = 4
    
    username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=username_length))
    domain = ''.join(random.choices(string.ascii_lowercase + string.digits, k=domain_length))
    
    email = f"{username}@{domain}.com"
    email_padded = email.ljust(14)

    return email_padded[:14]

with open('fixedwidth_data.txt', 'w') as output_file:
    output_file.write('H20240524TTTTTTTTTTT\n')
    for i in range(1000): 
        Amount = generate_random_amount()
        email = generate_random_email()
        UserID = str(i).zfill(4)
        output_file.write(f'{UserID}{Amount}{email}\n')
    output_file.write('F11WWW110000000000.00')