# Raizen_test
Foi realizada a extração dos dados, os quais foram registrados na pasta: Data/Raw
A conversão do arquivo foi feita através da transformação em Ods(libre office), assim, podendo acessar os dados que estavam na tabela dinâmica. Posteriormente, o arquivo foi salvo em .xls na pasta Data/Staging.
Como os dados estavam dispostos em várias abas separadas, foi necessário fazer a concatenação das abas solicitadas no exercicio.
Com a necessidade de obter o timestamp por linha, foi substituido os meses de "Jan,Fev..." para "1,2,3" para que, posteriormente, fosse concatenado com a coluna "ano"
Os meses e valores foram transpostos formando um banco de dados.
Para os demais campos tratados, alguns foram renomeados e outros extraidos de uma STRING.
Com a função GroupBY, ficou garantido a otimização dos dados, excluindo as colunas desnecessárias.
Foi criada a coluna "created_at", para a finalização dos campos solicitados.
O Arquivo foi carregado no Postgres


Padrão do erro ao converter:

Com a conversão do excel para Libreoffice, os dados foram desconfigurados, formando um padrão:

O maior valor de uma iinha representava a coluna total e seus subsequentes faziam a sequência dos meses, conforme exempo abaixo:

Rotulos Corretos	TOTAL	      Jan	      Fev	      Mar	      Abr	      Mai	      Jun	    Jul	      Ago	      Set	      Out	      Nov	      Dez
Arquivo conv.	    Jan	        Fev	      Mar	      Abr	      Mai	      Jun	      Jul	    Ago	      Set	      Out	      Nov	      Dez	      TOTAL
Valor	            164548,309	14001,15	13998,008	12592,121	13921,15	13962,816	13676,4	12461,124	13174,12	14419,916	13613,576	13934,106	14793,822

O Valor "164548,309" se encontra na coluna "jan", porém, ele representa a coluna "Total". O próximo valor(14001,15) portanto, será o mês de Janeiro e assim por diante conforme a linha Rotulos Corretos na tabela acima. Em qualquer posição que o valor total se encontra, a regra se mantém.

Com a função:
A coluna ANo tem um valor e poderia ser um problema. Para analisar e resolver o padrão, devemos pegar somente os dados a partir da coluna 5.Com as função abaixo:
idmax=df_temp.idxmax(axis=1)-Retorna o índice da coluna de maior valor
maxvalues=df_temp.max(axis=1)-retorna o maior valor

O maior valor representa a coluna total da tabela original.
Para consertar as colunas, deveria ser criada uma função apara aplicar em todas as linhas, na qual o idmax + 1 representa o mês de Jan, idmax+2 representa Fev e assim por diante. Atenção: quando o idmax for a última coluna, ela deve receber o indice 1. 

Para a resolução deste, o padrão foi encontrado, porém, não foi encontrado um modelo de resolução.
