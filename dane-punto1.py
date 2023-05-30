from mrjob.job import MRJob
from mrjob.step import MRStep

class SalaryStatistics(MRJob):
    
    def configure_args(self):
        super(SalaryStatistics, self).configure_args()
        self.add_file_arg('--columns', help='Path to columns.txt')
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salary,
                   reducer=self.reducer_average_salary),
            MRStep(mapper=self.mapper_get_employee,
                   reducer=self.reducer_employee_statistics)
        ]
    
    def mapper_get_salary(self, _, line):
        columns_path = self.options.columns
        with open(columns_path, 'r') as columns_file:
            columns = columns_file.readline().strip().split(',')
        
        idemp, sececon, salary, year = line.strip().split(',')
        yield sececon, float(salary)
    
    def reducer_average_salary(self, sececon, salaries):
        total_salaries = 0
        num_employees = 0
        for salary in salaries:
            total_salaries += salary
            num_employees += 1
        yield sececon, total_salaries / num_employees
    
    def mapper_get_employee(self, sececon, avg_salary):
        yield sececon, (1, avg_salary)
    
    def reducer_employee_statistics(self, sececon, employee_stats):
        total_employees = 0
        total_avg_salary = 0
        for stats in employee_stats:
            total_employees += stats[0]
            total_avg_salary += stats[1]
        yield sececon, (total_employees, total_avg_salary)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salary,
                   reducer=self.reducer_average_salary),
            MRStep(mapper=self.mapper_get_employee,
                   reducer=self.reducer_employee_statistics)
        ]

if __name__ == '__main__':
    SalaryStatistics.run()