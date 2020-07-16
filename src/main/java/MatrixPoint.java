import java.util.Random;

public class MatrixPoint {
    private int x;
    private int y;
    private int value;
    Random r = new Random();

    public MatrixPoint(int x, int y){
        this.x = x;
        this.y = y;
        this.value = r.nextInt(100-1) + 1;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String toString(){
        return x+","+y+","+value;
    }
}
