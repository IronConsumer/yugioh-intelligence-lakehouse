import os
import sys
from pyspark.sql import DataFrame

def showout(df : DataFrame, filename: str = 'output', lines: int = 20):
    """
    Capture la sortie de df.show() et l'enregistre dans un fichier texte.
    """
    target_dir = os.path.join("tests")
    
    os.makedirs(target_dir, exist_ok=True)
    
    file_path = os.path.join(target_dir, f"{filename}.txt")
    
    df_string = df._jdf.showString(lines, 200, False)
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(df_string)
        
    print(f"✅ Aperçu des données enregistré dans : {file_path}")


def clear():
    """
    Efface le contenu actuel du terminal.
    Fonctionne sur Windows (cls) et Linux/Mac (clear).
    """
    # 'nt' signifie Windows, sinon on considère que c'est du type Unix
    os.system('cls' if os.name == 'nt' else 'clear')

def restart():
    """
    Relance le processus Python actuel. 
    Utile pour vider la mémoire ou recharger tous les modules.
    """
    print("🔄 Nettoyage et redémarrage de la session...")
    
    executable = sys.executable
    args = sys.argv
    
    if not args or args[0] == '' or not os.path.exists(args[0]):
        os.execl(executable, executable)
    else:
        os.execv(executable, [executable] + args)