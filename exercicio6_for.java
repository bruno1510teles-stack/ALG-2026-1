import java.util.Scanner;

public class exercicio6_for {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        int c1 = 0, c2 = 0, c3 = 0, c4 = 0;
        int nulos = 0, brancos = 0;

        for (int cont = 1; cont <= 10; cont++) {
            System.out.print("Digite o voto do eleitor " + cont + ": ");
            int voto = scanner.nextInt();

            switch (voto) {
                case 1:
                    c1++;
                    break;
                case 2:
                    c2++;
                    break;
                case 3:
                    c3++;
                    break;
                case 4:
                    c4++;
                    break;
                case 5:
                    nulos++;
                    break;
                case 6:
                    brancos++;
                    break;
                default:
                    System.out.println("Voto inválido!");
                    cont--; // repete o eleitor
            }
        }

        int totalVotos = c1 + c2 + c3 + c4 + nulos + brancos;

        double percNulos = (double) nulos / totalVotos * 100;
        double percBrancos = (double) brancos / totalVotos * 100;

        System.out.println("\nResultado da eleição:");
        System.out.println("Candidato 1: " + c1 + " votos");
        System.out.println("Candidato 2: " + c2 + " votos");
        System.out.println("Candidato 3: " + c3 + " votos");
        System.out.println("Candidato 4: " + c4 + " votos");
        System.out.println("Votos nulos: " + nulos);
        System.out.println("Votos em branco: " + brancos);

        System.out.println("Percentual de nulos: " + percNulos + "%");
        System.out.println("Percentual de brancos: " + percBrancos + "%");

        scanner.close();
    }
}
