from math import ceil, log, floor

def ceil_power_of_ten(n):
    exp = log(n, 10)
    print("exp: ", exp)
    exp = floor(exp)
    print("exp: ", exp)
    return 10**exp


if __name__ == "__main__":
    print("870: ", ceil_power_of_ten(870))
    print("3: ", ceil_power_of_ten(3))
    print("87: ", ceil_power_of_ten(87))
    print("100: ", ceil_power_of_ten(100))

    num = 230
    index = (num // 10) % 10
    print("num 230 belongs in: ", index)



    