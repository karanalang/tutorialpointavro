package tutorialspoint.com;

import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/*
 * Sample code to serialization by Generating a class
 * Code to generate java file (based on the schema file - emp.avsc)-->
 * java -jar jars/avro-tools-1.8.2.jar compile schema schema/emp.avsc with_code_gen/ 
 * 
 * */

public class Serialize {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		System.out.println(" Serialize.java .. in main");
		
		emp e1 = new emp();
		e1.setName("emp1");
		e1.setAge(30);
		e1.setId(001);
		e1.setSalary(30000);
		e1.setAddress("cupertino");
		
		emp e2 = new emp();
		e2.setName("emp2");
		e2.setAge(35);
		e2.setId(002);
		e2.setSalary(40000);
		e2.setAddress("cupertino");
		
		emp e3 = new emp();
		e3.setName("emp3");
		e3.setAge(40);
		e3.setId(003);
		e3.setSalary(50000);
		e3.setAddress("cupertino");
		
		System.out.println("creating DaturmWriter & DataFileWriter ");
		
		//Instantiate DatumWriter class
		//reads the schema & determines in-memory representation 
		DatumWriter<emp> empDatumWriter = new SpecificDatumWriter<emp>(emp.class);
		
		System.out.println("AFTER creating DaturmWriter & DataFileWriter 1 ");
		
		//writes the schema + serialized data of record (conforming to schema) to a file
		DataFileWriter<emp> empFileWriter = new DataFileWriter<emp>(empDatumWriter);
		
		System.out.println("AFTER creating DaturmWriter & DataFileWriter  2");
		
		empFileWriter.create(e1.getSchema(), new File("/Users/karanalang/Documents/Technology/ApacheAvro/EmpAvro/with_code_gen/emp.avro"));
		
		System.out.println("AM HERE 1");
		
		//add all the created records to the file using append()
		empFileWriter.append(e1);
		empFileWriter.append(e2);
		empFileWriter.append(e3);
		
		System.out.println("AM HERE 2");
		
		empFileWriter.close();
		
		System.out.println(" Data serialized successfully ");
		
	}

}
