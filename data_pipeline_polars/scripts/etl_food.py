# Imports
from functions import DataProcess
import polars as pl
import re


def rename_columns(df_json, df_csv):
    """
    Renomeia colunas dos DataFrames antes de concatenar.
    """
    rename_columns_df1 = {'title': 'recipeTitle',
                    'rating': 'rating',
                    'calories': 'calories',
                    'protein': 'proteinGrams',
                    'fat': 'fatGrams',
                    'sodium': 'sodiumMg',
                    '22-minute meals': 'twentyTwoMinuteMeals',
                    '3-ingredient recipes': 'threeIngredientRecipes',
                    '30 days of groceries': 'thirtyDaysOfGroceries',
                    'advance prep required': 'advancePrepRequired',
                    'dairy free': 'dairyFree',
                    'fat free': 'fatFree',
                    'low cal': 'lowCal',
                    'low carb': 'lowCarb', 
                    'low cholesterol': 'lowCholesterol',
                    'low fat': 'lowFat',
                    'low sodium': 'lowSodium',
                    'low sugar': 'lowSugar',
                    'no sugar added': 'zeroSugar',
                    'no-cook': 'noCook',
                    'quick & easy': 'quickEasy',
                    'quick and healthy': 'quickHealthy',
                    'soy free': 'soyFree'
    }

    rename_columns_df2 = {
        'title': 'recipeTitle',
        'rating': 'rating',
        'calories': 'calories',
        'protein': 'proteinGrams',
        'fat': 'fatGrams',
        'sodium': 'sodiumMg',
        'directions': 'directions',
        'desc': 'desc',
        'ingredients': 'ingredients',
    }

    df_csv = df_csv.rename(rename_columns_df1)
    df_json = df_json.rename(rename_columns_df2)

    return df_json, df_csv


def match_percent(df_json, df_csv):
    """
    Calcula a porcentagem de correspondência entre os títulos das receitas nos dois DataFrames.
    """
    common_titles = df_csv["recipeTitle"].is_in(df_json["recipeTitle"]).sum()
    total_titles = df_csv["recipeTitle"].n_unique()
    return (common_titles / total_titles) * 100


def concat_encode_df(df_a, df_b):
    """
    Concatena dois DataFrames e adiciona um rótulo único para `recipeTitle`.
    """
    df_combined = pl.concat([df_a, df_b], how="diagonal")
    df_combined = df_combined.with_columns(
        pl.col("recipeTitle").rank("dense").alias("recipe_title_encoded")
    )
    return df_combined


def clean_null_match(df_combined):
    """
    Limpa valores nulos e realiza o preenchimento forward/backward.
    """
    # Identificar as colunas para processamento, excluindo 'recipeTitleEncoded' e 'recipeTitle'
    columns_to_process = [col for col in df_combined.columns if col not in ['recipeTitleEncoded', 'recipeTitle']]

    # Construir o DataFrame preenchido
    agg_operations = [
        pl.col("recipeTitle").first().alias("recipeTitle")
    ]

    # Adicionar operações de preenchimento para todas as outras colunas dinamicamente
    agg_operations.extend([
        pl.col(column_name).fill_null(strategy="forward")
                        .fill_null(strategy="backward")
                        .first().alias(column_name)
        for column_name in columns_to_process
    ])

    df_filled = (
        df_combined
        .group_by("recipeTitleEncoded", maintain_order=True)
        .agg(agg_operations)
    )

    return df_filled


# Function to parse ingredients
def parse_ingredient(line):
    match = re.match(r"^([\d/]+)\s+(\w+)\s+(.+)$", line)
    if match:
        quantity = match.group(1)
        type_medida = match.group(2)
        ingredient = match.group(3)
        return quantity, type_medida, ingredient
    else:
        return None, None, line
    

def process_df_ingredients(df):
    columns_ingredients = ['recipeTitleEncoded', 'recipeTitle', 'calories', 'ingredients']
    df_ingredients = df[columns_ingredients]

    # Explode the ingredients list, parse each line, and build the final DataFrame
    df_ingredients = (
        df_ingredients.explode("ingredients")  # Expand the list of ingredients into individual rows
        .with_columns([
            pl.col("ingredients").map_elements(lambda x: parse_ingredient(x)[0]).alias("quantity"),
            pl.col("ingredients").map_elements(lambda x: parse_ingredient(x)[1]).alias("type"),
            pl.col("ingredients").map_elements(lambda x: parse_ingredient(x)[2]).alias("ingredient_for_recipe"),
        ])
        .drop("ingredients")  # Remove the original ingredients column if no longer needed
    )


if __name__ == "__main__":

    # Caminho para os dados
    file_path_csv = r"C:\Users\fabia\Documents\ETL\data_pipeline_polars\data_raw\2\epi_r.csv"
    file_path_json = r"C:\Users\fabia\Documents\ETL\data_pipeline_polars\data_raw\2\full_format_recipes.json"
    docker_compose_path = r"C:\Users\fabia\Documents\ETL\data_pipeline_polars\docker-compose.yml"
    conection_string = DataProcess.get_db_config(docker_compose_path)

    # Instanciando a classe e carregando os dados
    csv_processor = DataProcess(file_path_csv, 'csv')
    json_processor = DataProcess(file_path_json, 'json')


    df_csv = csv_processor.df
    df_json = json_processor.df

    # Filtrar colunas importantes no CSV
    columns = [
           'title', 'rating', 'calories', 'protein', 'fat', 'sodium', '22-minute meals', '3-ingredient recipes', '30 days of groceries',
           'advance prep required', 'dairy', 'dairy free', 'egg','digestif','dinner', 'fat free', 'fish', 'grill', 'healthy','low cal',
           'low carb', 'low cholesterol', 'low fat','low sodium','low sugar','lunch', 'meat','no sugar added','no-cook','pork', 'quick & easy',
           'quick and healthy','soy','soy free','vegan','vegetarian'
    ]
    df_csv = df_csv.select(columns)

    # Renomear colunas
    df_json, df_csv = rename_columns(df_json, df_csv)

    # Calcula a correspondência de títulos entre os dois DataFrames
    match_percentage = match_percent(df_json, df_csv)
    print(f"Porcentagem de correspondência: {match_percentage:.2f}%")

    # Combina os dois DataFrames
    df_combined = concat_encode_df(df_json, df_csv)

    if match_percentage < 100:
        print("A correspondência não está completa. O DataFrame combinado pode conter valores nulos.")

    # Limpeza de valores nulos
    df_cleaned = clean_null_match(df_combined)
    df_cleaned = df_cleaned.with_columns(
        pl.col("directions").list.join(" ").alias("directions")
    )


    df_ingredients = process_df_ingredients(df_cleaned)
    table_name = 'ingredients'
    data_to_insert = df_ingredients.write_database(
        table_name=table_name,
        connection=conection_string,
        engine="sqlalchemy",
        if_table_exists="replace"
    )  
    
    print(f'subiu {table_name}')
    
    # process how to do recipes
    columns_directions = ['recipeTitleEncoded', 'recipeTitle', 'directions', 'desc']
    df_directions = df_cleaned[columns_directions]
    df_directions = df_directions.fill_null("nao informado")
    df_cleaned = df_cleaned.drop("directions")
    df_cleaned = df_cleaned.drop("ingredients")
    df_cleaned = df_cleaned.drop("categories")

    table_name = 'directions_recipes'
    data_to_insert_desc = df_directions.write_database(
        table_name=table_name,
        connection=conection_string,
        engine="sqlalchemy",
        if_table_exists="replace"
        )
    
    print(f'subiu {table_name}')

    #update recipes
    table_name = 'recipes_all'
    data_to_insert_desc = df_directions.write_database(
        table_name=table_name,
        connection=conection_string,
        engine="sqlalchemy",
        if_table_exists="replace"
        )
    
    print(f'subiu {table_name}')