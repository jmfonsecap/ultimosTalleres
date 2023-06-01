
from mrjob.job import MRJob

class AverageSalaryPerSececon(MRJob):

    def mapper(self, _, line):
        for w in line.split():
            filing = w.split(',')
            sececon = filing[1]
            salary = float(filing[2])
            yield sececon, salary

    def reducer(self, key, values):
        total_salary = 0
        count = 0

        for salary in values:
            total_salary += salary
            count += 1

        average_salary = total_salary / count

        yield key, average_salary

if __name__ == '__main__':
    AverageSalaryPerSececon.run()
