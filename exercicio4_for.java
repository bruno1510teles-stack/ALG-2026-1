import java.util.Scanner;

public class exercicio4_for {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        int somaAlturas = 0;
        int contador = 0;

        for (int cont = 1; cont <= 10; cont++) {
            System.out.println("Pessoa " + cont);

            System.out.print("Digite a idade: ");
            int idade = scanner.nextInt();

            System.out.print("Digite a altura (em cm): ");
            int altura = scanner.nextInt();

            if (idade > 50) {
                somaAlturas += altura;
                contador++;
            }
        }

        if (contador > 0) {
            double media = (double) somaAlturas / contador;
            System.out.println("Média das alturas das pessoas com mais de 50 anos: " + media);
        } else {
            System.out.println("Nenhuma pessoa com mais de 50 anos foi informada.");
        }

        scanner.close();
    }
}
