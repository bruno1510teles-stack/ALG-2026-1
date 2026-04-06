import java.util.Scanner;

public class exercicio7_for {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        int maiores50 = 0;
        int somaAlturas = 0;
        int contadorAltura = 0;
        int pesoMenor40 = 0;

        for (int cont = 1; cont <= 10; cont++) {
            System.out.println("Pessoa " + cont);

            System.out.print("Digite a idade: ");
            int idade = scanner.nextInt();

            System.out.print("Digite a altura (em cm): ");
            int altura = scanner.nextInt();

            System.out.print("Digite o peso (em kg): ");
            double peso = scanner.nextDouble();

            // a) maiores de 50 anos
            if (idade > 50) {
                maiores50++;
            }

            // b) altura entre 10 e 20 anos
            if (idade >= 10 && idade <= 20) {
                somaAlturas += altura;
                contadorAltura++;
            }

            // c) peso inferior a 40 kg
            if (peso < 40) {
                pesoMenor40++;
            }

            System.out.println();
        }

        // média das alturas
        double mediaAltura = 0;
        if (contadorAltura > 0) {
            mediaAltura = (double) somaAlturas / contadorAltura;
        }

        // porcentagem peso < 40
        double porcentagemPeso = (pesoMenor40 / 10.0) * 100;

        System.out.println("Quantidade de pessoas com mais de 50 anos: " + maiores50);
        System.out.println("Média das alturas (10 a 20 anos): " + mediaAltura);
        System.out.println("Porcentagem com peso inferior a 40kg: " + porcentagemPeso + "%");

        scanner.close();
    }
}
