package tutorialspoint.com;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.io.DatumReader;

public class DeserializeUsingParsers {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		//Instantiating Schema.Parser class
		Schema schema = new Schema.Parser().parse(new File("/Users/karanalang/Documents/Technology/ApacheAvro/EmpAvro/schema/emp.avsc"));
		
		//
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("/Users/karanalang/Documents/Technology/ApacheAvro/EmpAvro/without_code_gen/emp_usingParsers.avro"),datumReader);
		
		GenericRecord emp = null;
		
		while(dataFileReader.hasNext()){
			//emp = dataFileReader.next(emp);
			emp = dataFileReader.next();
			System.err.println(" emp is " + emp);
		}
		
		System.out.println(" end of DeSerialization");
		
		
	}

}
