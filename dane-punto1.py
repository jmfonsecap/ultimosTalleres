from mrjob.job import MRJob
from mrjob.step import MRStep

class SalaryStatistics(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salary,
                   reducer=self.reducer_average_salary),
            MRStep(mapper=self.mapper_get_employee,
                   reducer=self.reducer_employee_statistics)
        ]
    
    def mapper_get_salary(self, _, line):
        
        idemp, sececon, salary, year = line.strip().split(',')
        yield sececon, float(salary)
    
    def reducer_average_salary(self, sececon, salaries):
        total_salaries = 0
        num_employees = 0
        for salary in salaries:
            total_salaries += salary
            num_employees += 1
        yield sececon, total_salaries / num_employees
    
    def mapper_get_employee(self, _, line):
        idemp, sececon, salary, year = line.strip().split(',')

        yield idemp, float(salary)
    
    def reducer_employee_statistics(self, idemp, salaries):
        total_salaries = 0
        num_jobs = 0
        for salary in salaries:
            total_salaries += salary
            num_jobs += 1
        yield idemp, total_salaries / num_jobs
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salary,
                   reducer=self.reducer_average_salary),
            MRStep(mapper=self.mapper_get_employee,
                   reducer=self.reducer_employee_statistics)
        ]

if __name__ == '__main__':
    SalaryStatistics.run()