# Dados do Parlamento Português

Código de um pequeno trabalho freelancer que fiz para capturar vários dados disponíveis no site do
parlamento português. Basicamente, todos os deputados ativos, efetivos definitivos e efetivos temporários
em todos os dias desde o início da VIII legislatura até o final da XIII, as biografias, atividades
e registros de interesse desses deputados. Exporto-os em planilhas, disponíveis [aqui]().
Os dados crus estão armazenados no [átomo persistente](https://github.com/alandipert/enduro) em `db.atom.zip` (precisa extrair).

## Planilhas

Ver `/out`

```
'Atividades IX.xlsx'
'Atividades VIII.xlsx'
'Atividades XIII.xlsx'
'Atividades XII.xlsx'
'Atividades XI.xlsx'
'Atividades X.xlsx'
 Biografias.xlsx
'Interesse XIII.xlsx'
'Interesse XII.xlsx'
'Interesse XI.xlsx'
'IX Legislatura.xlsx'
'VIII Legislatura.xlsx'
'XIII Legislatura.xlsx'
'XII Legislatura.xlsx'
'XI Legislatura.xlsx'
'X Legislatura.xlsx'
```

### Procedimento:

Planilha de Legislatura:

Crio um robô que busca, na página de  busca avançada de Parlamentares, todos os dias entre o início da VIII legislatura até o final da XIII para três situações: ativo, efetivo definitivo e efetivo temporário. Com isso, há ~ 1 millhão de entradas individuais. Compilo esses dados agrupando por períodos contíguos onde cada deputado esteve presente, sem alteração no Partido ou Situação. Além disso, guardo os BIDs (identificadores individuais) de cada parlamentar.

Planilha de Biografia:

O robô visita a página de Biografia de cada deputado, guardando todas as informações apresentadas e a tabela com informações sobre cada legislatura de que participou. Além disso, essa tabela contém os links para as páginas de registro de interesses para todas as legislaturas em que houve a declaração. Gero uma planilha com todas as informações de todos os deputados.

Planilha de Registro de Interesses:

Visito todos  os links coletados na etapa anterior, salvando todo o conteúdo.

Planilha de Atividades:

Para cada legislatura, uso os dados da primeira etapa para verificar quais deputados tiveram alguma atividade no período. Visito a página de atividades deles nessa legislatura e salvo tudo. Compilo as informações por seção, cada seção gerando uma planilha.

## Uso

```clojure
  ; setup produção
  (def headless? true)
  (init-drivers! 16) ;; 12 chrome drivers paralelos
  (pmap click-cookies! (:pool @drivers))

  ; setup dev, um chromedriver só
  (init-drivers! 1)
  (pmap click-cookies! (:pool @drivers))
  (def driver (first (:pool @drivers)))

  ; tarefas de scraping definitivo
  (def scraper (scrape-active! "1999-10-25" "2019-10-24"))
  (def scraper (scrape-efetivo-definitivo! "1999-10-25" "2019-10-24"))
  (def scraper (scrape-efetivo-temporario! "1999-10-25" "2019-10-24"))
  (def scraper (scrape-bios!))
  (def scraper (scrape-interesse-v1!))
  (def scraper (scrape-interesse-v2!))
  (def scraper (scrape-atividade-v2!))

  ; cancelar scraping.
  (cancel-scrape! scraper)
```

## Libs usadas

* [clj-time](https://github.com/clj-time/clj-time)
* [Diehard](https://github.com/sunng87/diehard)
* [Docjure](https://github.com/mjul/docjure)
* [Enduro](https://github.com/alandipert/enduro)
* [Etaoin](https://github.com/igrishaev/etaoin)
* [Hickory](https://github.com/davidsantiago/hickory)
* [Manifold](https://github.com/ztellman/manifold)
* [Timbre](https://github.com/ptaoussanis/timbre)

## Licença

Distributed under the MIT License