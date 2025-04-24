flook = set()

if flook.add(1):
    print("add 1")
if flook.add(1):
    print("add 1 again")
if flook.add(3):
    print("add 3")

print(len(flook))
print(next(iter(flook)))

