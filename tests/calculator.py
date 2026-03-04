class Calculator:
    def __init__(self, num1, num2): # always have an init function
        self.num1 = num1
        self.num2 = num2

    def get_add(self):
        return self.num1 + self.num2

    def get_subtraction(self):
        return self.num1 - self.num2
    
    def get_multiplication(self):
        return self.num1 * self.num2
    
    def get_division(self):
        return self.num1 / self.num2
    
if __name__ == "__main__":
    myCalc = Calculator(num1 = 10, num2 = 20)
    add_answer            = myCalc.get_add()
    subtraction_answer    = myCalc.get_subtraction()
    multiplication_answer = myCalc.get_multiplication()
    division_answer       = myCalc.get_division()
    print(add_answer, subtraction_answer, multiplication_answer, division_answer)