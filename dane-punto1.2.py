
from mrjob.job import MRJob
class AverageSalaryPerEmployee(MRJob):

    def mapper(self, _, line):
        for w in line.split():
            filing = w.split(',')
            idemp = filing[0]
            salary = float(filing[2])
            yield idemp, salary

    def reducer(self, key, values):
        total_salaries = 0
        count = 0

        for salary in values:
            total_salaries += salary
            count += 1

        yield key, total_salaries / count

if __name__ == '__main__':
    AverageSalaryPerEmployee.run()