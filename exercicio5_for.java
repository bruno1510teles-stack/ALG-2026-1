import java.util.Scanner;

public class exercicio5_for {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        int aprovados = 0;
        int exame = 0;
        int reprovados = 0;
        double somaMedias = 0;

        for (int cont = 1; cont <= 6; cont++) {
            System.out.println("Aluno " + cont);

            System.out.print("Digite a primeira nota: ");
            double nota1 = scanner.nextDouble();

            System.out.print("Digite a segunda nota: ");
            double nota2 = scanner.nextDouble();

            double media = (nota1 + nota2) / 2;
            somaMedias += media;

            System.out.println("Média: " + media);

            if (media <= 3) {
                System.out.println("REPROVADO");
                reprovados++;
            } else if (media < 7) {
                System.out.println("EXAME");
                exame++;
            } else {
                System.out.println("APROVADO");
                aprovados++;
            }

            System.out.println();
        }

        double mediaClasse = somaMedias / 6;

        System.out.println("Total de aprovados: " + aprovados);
        System.out.println("Total de exame: " + exame);
        System.out.println("Total de reprovados: " + reprovados);
        System.out.println("Média da classe: " + mediaClasse);

        scanner.close();
    }
}
