# Curso ST0263 Tópicos Especiales en Telemática
# Realizacion laboratorio 5
* Se crea un bucket s3 como se indica
![Imagen de bucket](https://github.com/jmfonsecap/ultimosTalleres/blob/main/s3.jpeg)

* Luego se crea el cluster EMR como se especifica en el laboratorio


![Imagen de creacion EMR](https://github.com/jmfonsecap/ultimosTalleres/blob/main/summary1.jpeg)


![Imagen de creacion EMR](https://github.com/jmfonsecap/ultimosTalleres/blob/main/summary2.jpeg)


![Imagen de creacion EMR](https://github.com/jmfonsecap/ultimosTalleres/blob/main/summary3.jpeg)


![Imagen de creacion EMR](https://github.com/jmfonsecap/ultimosTalleres/blob/main/summary4.jpeg)


![Imagen de creacion EMR](https://github.com/jmfonsecap/ultimosTalleres/blob/main/summary5.jpeg)

* Ahora se hacce la conexion con hadoop como indicado
![Imagen cmd hadoop](https://github.com/jmfonsecap/ultimosTalleres/blob/main/ConectarseaEMR.jpeg)

* Y se levanta el tunel para accederlo desde la interfaz web
![Imagen cmd levantamiento tunel](https://github.com/jmfonsecap/ultimosTalleres/blob/main/abrirtunel.jpeg)

![Imagen conexion hadoop via web](https://github.com/jmfonsecap/ultimosTalleres/blob/main/hueConsole.jpeg)


* Ahora se crea test con el archivo text1.txt

![Imagen text1](https://github.com/jmfonsecap/ultimosTalleres/blob/main/crear.textHue.jpeg)


![Imagen test desde consola](https://github.com/jmfonsecap/ultimosTalleres/blob/main/lsUserAdmin.jpeg)

![a](https://github.com/jmfonsecap/ultimosTalleres/blob/main/lsTest.jpeg)

* Ahora se crea el test2 desde consola con el archivo file2.txt

![Imagen text2 desde la consola](https://github.com/jmfonsecap/ultimosTalleres/blob/main/lsAdmin.jpeg)

![Imagen text2 desde la consola](https://github.com/jmfonsecap/ultimosTalleres/blob/main/CopyFromLocal.jpeg)

![Imagen text2 desde la consola](https://github.com/jmfonsecap/ultimosTalleres/blob/main/lsTest2.jpeg)
* Y ya esta el laboratorio 5!


# Laboratorio: Map/Reduce en Python con MRJOB.
* Primero realizaremos el primer reto que es el despliegue del EMR via AWS CLI. Para esto debemos instalar AWS
![Instalacion AWS](https://github.com/jmfonsecap/ultimosTalleres/blob/main/InstallAWS.jpeg)
* Ahora se debe configurar las credenciales para que se pueda crear el cluster. Esto lo haremos metiendonos al learner lab, undiendo AWS details y yendo a AWS CLI

![Imagen aws CLI](https://github.com/jmfonsecap/ultimosTalleres/blob/main/AWSCLICredentials.jpeg)
* Ahora esto se debe copiar y pegar en  ~/.aws/credentials

![Imagen credentials](https://github.com/jmfonsecap/ultimosTalleres/blob/main/Credentials.jpeg)
* Luego deberomos correr el siguiente comando
```sh
     aws emr create-cluster --release-label emr-6.10.0 --instance-type m4.large --instance-count 3 --log-uri s3://jmfonsecap-lab-emr/logs --use-default-roles --ec2-attributes KeyName=emr-key,SubnetId=subnet-0386f1f316823a038 --no-termination-protected
```
* Se debe de tener en cuenta que en el log-url se pone la url correspondiente al log de s3, el KeyName debe ser la .pem que creamos y la subnet debe ser de la vpc.

![Cluster CLI](https://github.com/jmfonsecap/ultimosTalleres/blob/main/ClusterCLI.jpeg)
* Ya debemos esperar a que el emr despliegue y que este en estado "waiting"
* Cuando este listo, nos conectamos por medio del siguiente comando
```sh
      ssh -i emr-key.pem hadoop@ec2-54-89-143-7.compute-1.amazonaws.com
```
* Y ya quedo el primer reto
* Ahora es neccesario instalarle git al EMR, para esto se corre el comando
```sh
      sudo yum install git
```
* Luego se debe copiar el repositorio del laboratorio y se debe acceder a la carpeta wordcount y correr los comandos dados en el laboratorio
```sh
     cd st0263-2023-1/Laboratorio\ N6-MapReduce/wordcount/
     python wordcount-local.py /home/hadoop/st0263-2023-1/datasets/gutenberg-small/*.txt | sudo tee salida-serial.txt > /dev/null
     more salida-serial.txt
```

* Se altero la linea de codigo ya que no tenia permiso de creación

![Imagen resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/ResultadoLocalSinMrJob.jpeg)

* Ahora, se instala python3 y mrjob.

```sh
	sudo yum install python3-pip
	sudo pip3 install mrjob
````

* Probar mrjob python local:

```sh
	cd wordcount
	python wordcount-mr.py ./datasets/gutenberg-small/*.txt
````

![Resultado local](https://github.com/jmfonsecap/ultimosTalleres/blob/main/resultadoWordCountlocal.jpeg)
* Ahora hacemos el segundo reto propuesto
* Para eso tenemos que copiar el dataset en nuestro emr
*

```sh
	hdfs dfs -copyFromLocal /home/hadoop/st0263-2023-1/datasets/ /user/admin/
```
* Ahora que lo tenemos copiado corremos el comando
```sh
	python wordcount-mr.py hdfs:///user/admin/datasets/gutenberg-small/*.txt -r hadoop --output-dir hdfs:///user/admin/result3 
```

![Resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/resultadoWordCountMrJob.jpeg)

* Y ese es el reto 2!

# Reto de Programación en Map/Reduce

* Los retos se resuelven con los programas que estan en este repositorio. Pero para eso debemos clonarlo al EMR
* Luego entonces se va a la carpeta del repositorio
```sh
	cd ultimosTalleres
````
* Y a partir de ahi podemos comenzar a correr los comandos

1. Se tiene un conjunto de datos, que representan el salario anual de los empleados formales en Colombia por sector económico, según la DIAN. [datasets de ejemplo](../datasets/otros)

    *  La estructura del archivo es: (sececon: sector económico) (archivo: dataempleados.csv)

        idemp,sececon,salary,year

        3233,1234,35000,1960
        3233,5434,36000,1961
        1115,3432,34000,1980
        3233,1234,40000,1965
        1115,1212,77000,1980
        1115,1412,76000,1981
        1116,1412,76000,1982
	
     1. El salario promedio por Sector Económico (SE)
        El programa se llama dane-punto1.1.py y el resultado estara en el folder /user/admin/reto/punto1/punto1
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto1.1.jpeg)
	
        2. El salario promedio por Empleado
        El programa se llama dane-punto1.2.py y el resultado estara en el folder /user/admin/reto/punto1/punto2
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto1.2.jpeg)
	
        3. Número de SE por Empleado que ha tenido a lo largo de la estadística
	El programa se llama dane-punto1.3.py y el resultado estara en el folder /user/admin/reto/punto1/punto3
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto1.3.jpeg)

2. Se tiene un conjunto de acciones de la bolsa, en la cual se reporta a diario el valor promedio por acción, la estructura de los datos es (archivo: dataempresas.csv):

    company,price,date

    exito,77.5,2015-01-01
    EPM,23,2015-01-01
    exito,80,2015-01-02
    EPM,22,2015-01-02
   
   1. Por acción, dia-menor-valor, día-mayor-valor
        
	El programa se llama dane-punto2.1 y el resultado estara en el folder /user/admin/reto/punto2/punto1
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto2.1.jpeg)
	
        2. Listado de acciones que siempre han subido o se mantienen estables.
        El programa se llama dane-punto2.2 y el resultado estara en el folder /user/admin/reto/punto2/punto3
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto2.2.jpeg)
	
3. DIA NEGRO: Saque el día en el que la mayor cantidad de acciones tienen el menor valor de acción (DESPLOME), suponga una inflación independiente del tiempo.
	
	El programa se llama dane-punto2.3 y el resultado estara en el folder /user/admin/reto/punto2/punto3
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto2.3.jpeg)

3. Sistema de evaluación de películas (archivo: datapeliculas.csv): Se tiene un conjunto de datos en el cual se evalúan las películas con un rating, con la siguiente estructura:

    User,Movie,Rating,Genre,Date

    166,346,1,accion,2014-03-20
    298,474,4,accion,2014-03-20
    115,265,2,accion,2014-03-20
    253,465,5,accion,2014-03-20
    305,451,3,accion,2014-03-20

        a. Número de películas vista por un usuario, valor promedio de calificación
        El programa se llama peliculas.a y el resultado estara en el folder /user/admin/reto/punto3/puntoa
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto3a.jpeg)
	
        b. Día en que más películas se han visto
        El programa se llama peliculas.b y el resultado estara en el folder /user/admin/reto/punto3/puntob
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto3b.jpeg)
	
        c. Día en que menos películas se han visto
        El programa se llama peliculas.c y el resultado estara en el folder /user/admin/reto/punto2/puntoc
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto3c.jpeg)
	
	d. Número de usuarios que ven una misma película y el rating promedio
        El programa se llama peliculas.d y el resultado estara en el folder /user/admin/reto/punto2/puntod
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto3d.jpeg)
	
	e. Día en que peor evaluación en promedio han dado los usuarios
        El programa se llama peliculas.e y el resultado estara en el folder /user/admin/reto/punto2/puntoe
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto3e.jpeg)
	
	f. Día en que mejor evaluación han dado los usuarios
        El programa se llama peliculas.f y el resultado estara en el folder /user/admin/reto/punto2/puntof
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto3f.jpeg)
	
	g. La mejor y peor película evaluada por genero
        El programa se llama peliculas.g y el resultado estara en el folder /user/admin/reto/punto2/puntog
	
	![resultado](https://github.com/jmfonsecap/ultimosTalleres/blob/main/punto3g.jpeg)



