import numpy as np
import pandas as pd

class Preprocessor:
    
    def __init__(self,file_path):
        self.df = pd.read_csv(file_path)

    def set_date(self,fill_na):
        length_first = self.df.shape[0]
        print(length_first)
        # Cases
        coming_soon_indices = self.df.loc[self.df["Release Date"] == "Coming soon"].index # Coming soon değerleri.
        to_be_announced_indices = self.df.loc[self.df["Release Date"] == "To be announced"].index # To be announced değerleri.
        q_indices = self.df.index[self.df['Release Date'].str.contains('Q',na=False)].tolist() # Q içeren değerler.
        year_indices = self.df.index[self.df['Release Date'].str.match(r'^\d{4}$',na=False)].tolist() # Sadece yıl içeren değerler.

        # Fill NaN or Drop
        if fill_na:
            self.df.loc[coming_soon_indices, 'Release Date'] = np.nan
            self.df.loc[to_be_announced_indices, 'Release Date'] = np.nan
            self.df.loc[year_indices, 'Release Date'] = np.nan
            self.df.loc[q_indices, 'Release Date'] = np.nan
        else:
            self.df.drop(coming_soon_indices, inplace=True)
            self.df.drop(to_be_announced_indices, inplace=True)
            self.df.drop(year_indices, inplace=True)
            self.df.drop(q_indices, inplace=True)

        # Regex Cases
        date_regex_1 = r"^\d{1,2}\s(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?),\s\d{4}$"
        date_regex_2 = r"^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{4}$"

        mask = self.df['Release Date'].str.match(date_regex_1,na=False)
        full_date_indices = self.df.index[mask].tolist()

        mask2 = self.df['Release Date'].str.match(date_regex_2,na=False)
        month_year_indices = self.df.index[mask2].tolist()

        self.df.loc[full_date_indices,"Release Date"] = pd.to_datetime(self.df.loc[full_date_indices, 'Release Date'], format='%d %b, %Y',errors='coerce')
        self.df.loc[month_year_indices,"Release Date"] = pd.to_datetime(self.df.loc[month_year_indices,"Release Date"])

        # Convert Release Date type to datetime.
        self.df['Release Date'] = pd.to_datetime(self.df['Release Date'], format='%Y-%m-%d', errors='coerce')

        # Drop non-convertible values
        nan_list = self.df.index[self.df["Release Date"].isna()].tolist()
        self.df.drop(nan_list, inplace=True) # 135 adet dönüştürülemeyen değer droplandı.
        self.df.reset_index(drop=True, inplace=True)
        length_last = self.df.shape[0]
        print(f"set_date aşamasında {(length_first - length_last)} satır veri kaybı oldu.")
        print("set_date has just runned.")

    def clean_price_column(self,column_name,fill_free=True,convert_column_type_to_float=True):
        self.df[column_name] = self.df[column_name].str.strip("$")
        self.df[column_name] = self.df[column_name].str.replace(",", "")
        if fill_free:
            self.df[column_name] = self.df[column_name].str.replace("Free", "0")
        if convert_column_type_to_float:
            self.df[column_name] = self.df[column_name].astype(float)
        print(f"clean_price_column has just runned for {column_name}.")

    def parse_minimum_requirements(self,keywords):
        length = self.df.shape[0]
        all_values = []
        for index in range(length):
            index_list = []
            val_list = self.df["Minimum Requirements"].values[index]
            try:
                val_list = [str(item).strip() for item in val_list.split("|")]
            except:
                # print(f"{index} {type(val_list)}")
                pass
            requirements_dict = {}
            for word in keywords:
                try:
                    if f"{word}:" in val_list:
                        # print(val_list)
                        storage_index = val_list.index(f"{word}:")
                        storage_value = val_list[storage_index+1]
                        requirements_dict.update({word:storage_value})
                        # print(requirements_dict)
                    else:
                        # print(f"{index} numaralı indexte f{word}: değeri yok.")
                        pass
                except:
                    # print(f"Bu indexte({index}) hata var:")
                    pass
            index_list.append(index)
            index_list.append(requirements_dict)
            all_values.append(index_list)
        print("parse_minimum_requirements has just runned.")
        return all_values

    def fix_storage(self):
        length_first = self.df.shape[0]
        self.df['Storage'] = self.df['Storage'].str.replace("available space", "", regex=False)
        ##
        error_indexes = []
        # "GB" içeren satırları işleyelim
        for index, value in self.df[self.df['Storage'].str.contains('GB',na=False)]['Storage'].items():
            try:
                # "GB" ifadesini silip sayısal değeri alalım ve 1024 ile çarpalım
                numeric_value = float(value.replace(' GB', ''))
                self.df.at[index, 'Storage'] = f"{int(numeric_value * 1024)} MB"
            except ValueError:
                # Hata alınan indeksi kaydedelim
                error_indexes.append(index)
        self.df = self.df.drop(error_indexes)
        ##
        self.df['Storage'] = self.df['Storage'].str.replace("MB", "", regex=False)
        ##
        filtered_indices = self.df[self.df['Storage'].str.contains(r'[^\w\s]', regex=True, na=False)].index
        will_be_deleted_list = []
        for index in filtered_indices:
            value = self.df.at[index, 'Storage']
            if "." in value:
                new_value = self.df.at[index, 'Storage'].replace('.', '')  # Noktaları sil
                self.df.at[index, 'Storage'] = new_value
            else:
                will_be_deleted_list.append(index)
        self.df = self.df.drop(will_be_deleted_list)
        ##
        error_indices_2 = []
        try:
            self.df['Storage'] = self.df['Storage'].astype(float)
        except ValueError as e:
            # Hata alınan indeksleri bul
            for index, value in self.df['Storage'].items():
                try:
                    _ = float(value)
                except ValueError:
                    error_indices_2.append(index)
        self.df = self.df.drop(error_indices_2)
        ##
        self.df['Storage'] = self.df['Storage'].astype(float)
        self.df.rename(columns={'Storage': 'Storage_mb'}, inplace=True)
        length_last = self.df.shape[0]
        print("fix_storage has just runned.")
        print(f"fix_storage aşamasında {(length_first - length_last)} satır veri kaybı oldu.")
        self.df.reset_index(drop=True, inplace=True)

    def fix_memory(self):
        length_first = self.df.shape[0]
        self.df['Memory'] = self.df['Memory'].str.replace("RAM", "", regex=False)
        ##
        error_indexes = []
        # "GB" içeren satırları işleyelim
        for index, value in self.df[self.df['Memory'].str.contains('GB',na=False)]['Memory'].items():
            try:
                # "GB" ifadesini silip sayısal değeri alalım ve 1024 ile çarpalım
                numeric_value = float(value.replace(' GB', ''))
                self.df.at[index, 'Memory'] = f"{int(numeric_value * 1024)} MB"
            except ValueError:
                # Hata alınan indeksi kaydedelim
                error_indexes.append(index)

        self.df['Memory'] = self.df['Memory'].str.replace("MB", "", regex=False)
        ##
        error_indices_2 = []
        try:
            self.df['Memory'] = self.df['Memory'].astype(float)
        except ValueError as e:
            # Hata alınan indeksleri bul
            for index, value in self.df['Memory'].items():
                try:
                    _ = float(value)
                except ValueError:
                    error_indices_2.append(index)

        self.df = self.df.drop(error_indices_2)
        self.df['Memory'] = self.df['Memory'].astype(float)
        self.df.rename(columns={'Memory': 'Memory_mb'}, inplace=True)
        length_last = self.df.shape[0]
        print("fix_memory has just runned.")
        print(f"fix_memory aşamasında {(length_first - length_last)} satır veri kaybı oldu.")
        self.df.reset_index(drop=True, inplace=True)


    def run_preprocessor(self):
        ##
        fill_na = False
        self.set_date(fill_na)
        ##
        self.clean_price_column("Original Price")
        self.clean_price_column("Discounted Price")
        ##
        """
        game_types_count_value = 5000 #5000
        self.parse_game_features(game_types_count_value)
        """
        ##
        # keywords = ["OS","Processor","Memory","Graphics","DirectX","Storage"]
        keywords = ["Memory","Storage"]
        all_values = self.parse_minimum_requirements(keywords)
        parsed_minimum_requirements_df = pd.DataFrame([dict(item[1], Index=item[0]) for item in all_values])
        self.df = pd.concat([self.df,parsed_minimum_requirements_df],axis=1)
        self.df.drop(columns=['Index'], inplace = True)
        ##
        self.fix_storage()
        ##
        self.fix_memory()
        ##
        """
        supported_tags_count_value = 15000
        self.parse_supported_tags(supported_tags_count_value)
        """

        ##
        """
        common_languages_count_value = 5000
        self.parse_supported_languages(common_languages_count_value)
        """
        return self.df