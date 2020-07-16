import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class QuestionFourDatasets {
    /*
    In	this	question, you	are	asked	to	create	two	files	M1	&	M2,	where	M1	represents	Matrix	1,	and
M2	represents	Matrix	2.	The	structure	of	each	file	is	as	illustrated	in	the	posted	slides	(3	columns
in	each	file).	Assume	the	dimensions	of	M1	and	M2	are	500x500.	Populate	the	files	with	random
integer	values	for	each	entry.

     */
    public static void main(String[] args) {
        try {

            File  m1 = new File("dataQ4/M1.txt");
            File m2 = new File("dataQ4/M2.txt");

            BufferedWriter writer1 = new BufferedWriter(new FileWriter(m1));
            BufferedWriter writer2 = new BufferedWriter(new FileWriter(m2));

            MatrixPoint matrix;

           // int x =0;
         //   int y =0;
            for(int x =0; x< 500; x++){
                for(int y =0; y< 500; y++){
                    matrix = new MatrixPoint(x, y);
                    writer1.write(matrix.toString());
                    writer1.newLine();
                    matrix = new MatrixPoint(x, y);
                    writer2.write(matrix.toString());
                    writer2.newLine();
                }
            }

          writer1.close();
          writer2.close();
        } catch (Exception e) {

        }

    }
}
