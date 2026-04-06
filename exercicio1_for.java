public class exercicio1_for {
    public static void main(String[] args) {
        for (int cont = 100; cont >= 1; cont--) {
            System.out.print(cont);
            
            if (cont > 1) {
                System.out.print(", ");
            }
        }
    }
}
