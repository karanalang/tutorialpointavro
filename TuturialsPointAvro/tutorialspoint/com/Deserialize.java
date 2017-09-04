package tutorialspoint.com;

import java.io.File;
import java.io.IOException;

import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileReader;

public class Deserialize {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

	//deserialize the objects
		DatumReader<emp> empDatumReader = new SpecificDatumReader<emp>(emp.class);
		
	//instatiating DataFileReader
		DataFileReader<emp> dataFileReader = new DataFileReader<emp>(new File("/Users/karanalang/Documents/Technology/ApacheAvro/EmpAvro/with_code_gen/emp.avro"), empDatumReader); 
		
		emp em = null;
		while(dataFileReader.hasNext()){
			em = dataFileReader.next();
			System.out.println("em => " + em);
			System.out.println("em age => " + em.getAge());
		}
		
		
		
	}

}
