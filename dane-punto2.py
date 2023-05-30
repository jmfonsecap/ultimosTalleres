from mrjob.job import MRJob

class SalaryAverageMR(MRJob):

    def mapper(self, _, line):
        # Dividir la línea en campos
        entries = line.split()
        for entry in entries:
            fields = line.split(',')
            # Obtener el sector económico y el salario
            sececon = fields[1]
            salary = int(fields[2])

            # Emitir clave: sector económico, valor: salario
            yield sececon, salary

    def reducer(self, key, values):
        # Calcular el salario promedio
        total_salary = 0
        count = 0

        for salary in values:
            total_salary += salary
            count += 1

        average_salary = total_salary / count

        # Emitir resultado: clave: sector económico, valor: salario promedio
        yield key, average_salary

if __name__ == '__main__':
    SalaryAverageMR.run()
