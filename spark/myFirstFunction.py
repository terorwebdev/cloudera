import sys

def calcSalary(totalHours, hourlyWage):
    if totalHours <= 40:
        totalWages = hourlyWage*totalHours
    else:
        overtime = totalHours - 40
        totalWages = hourlyWage*40 + (1.5*hourlyWage)*overtime
    return totalWages

def main(hours, wage):
    total = calcSalary(hours, wage)   
    print('Wages for %d hours at RM%.2f per hour are RM%.2f'%(hours,wage,total)) #2 decimal points


if __name__ == "__main__":
   hours = 40
   wage = 10  
   main(hours, wage)
