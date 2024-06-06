import random
from faker import Faker

out = []
names = []
addresses = []
fake = Faker()
for i in range(1000):
    names.append(fake.name())
    addresses.append(fake.address())
for i in range(10000000):
    out.append(f"{random.choice(names), random.choice(addresses)}")
    if i % 1000 == 0:
        print(i)

with open("out.csv", "w") as f:
    f.write("\n".join(out))
