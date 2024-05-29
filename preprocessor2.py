import pandas as pd
import numpy as np
from scipy import stats

class Preprocessor2:
    def __init__(self, file_path):
        self.df = pd.read_csv(file_path)

    def get_common_game_types(self,count_value):
        freq_dict = {}
        length = self.df.shape[0]
        for index in range(length):
            test_str = self.df["Game Features"].values[index]
            test_str = test_str.replace("[", "").replace("]", "").replace("'","")
            game_features_list = test_str.split(",")
            for game_feature in game_features_list:
                stripped_game_feature = game_feature.strip().lower()
                if not stripped_game_feature in freq_dict.keys(): # dil dict'te yok ise 1 ile başlat.
                    freq_dict.update({stripped_game_feature:1})
                else: #var ise mevcut durumunu 1 arttır.
                    freq_dict[stripped_game_feature] += 1
        # Sözlüğün öğelerini (anahtar ve değer çiftleri) bir liste halinde al
        items = freq_dict.items()
        # Değerlere göre büyükten küçüğe sırala
        sorted_items = sorted(items, key=lambda x: x[1], reverse=True)
        # verdiğimiz değere göre desteklenmiş diller.
        values_greater_than_count_value = [key.lower() for key, value in sorted_items if value > count_value]
        return values_greater_than_count_value

    def parse_game_features(self,game_types_count_value):
        common_game_types = self.get_common_game_types(game_types_count_value)
        length = self.df.shape[0]
        for common_game in common_game_types:
            self.df[f"is_{common_game}"] = np.zeros(length,dtype=int)
        for index in range(length):
            test_str = self.df["Game Features"].values[index]
            test_str = test_str.replace("[", "").replace("]", "").replace("'","")
            game_type_list = test_str.split(",")
            for game_type in game_type_list:
                game = game_type.strip().lower()
                if game in common_game_types:
                    self.df.at[index,f"is_{game}"] = 1


    def get_all_tag_frequencies(self,count_value):
        all_tags = []
        for tags in self.df["Popular Tags"]:
            if pd.notna(tags):
                tags_list = tags.replace("[", "").replace("]", "").replace("'", "").split(",")
                # Her bir etiketi boşlukla ayırılmış olarak 'all_tags' listesine eklendi
                all_tags.extend([tag.strip() for tag in tags_list])
        # Tüm etiketlerin frekanslarını hesaplamak için
        tag_frequencies = {}
        for tag in all_tags:
            if tag in tag_frequencies:
                tag_frequencies[tag] += 1
            else:
                tag_frequencies[tag] = 1
    # return tag_frequencies
        common_tags = [key.lower() for key, value in tag_frequencies.items() if value > count_value]
        return common_tags

    def parse_supported_tags(self,supported_tags_count_value):
        common_tags = self.get_all_tag_frequencies(supported_tags_count_value)
        length = self.df.shape[0]
        for tag in common_tags:
            self.df[f"is_{tag}"] = np.zeros(length, dtype=int)
        for index in range(length):
            test_str = self.df["Popular Tags"].values[index]
            if pd.notna(test_str):  # Eğer Popular Tags sütunu NaN değer içeriyorsa bu adımı atla
                test_str = test_str.replace("[", "").replace("]", "").replace("'", "")
                tag_list = test_str.split(",")
                for tag in tag_list:
                    tag = tag.strip().lower()
                    if tag in common_tags:
                        self.df.at[index, f"is_{tag}"] = 1

    # Supported Languages sütununda hiç NaN değer olmadığı için 1 dışındaki değerleri 0 ile doldurdum.
    def get_common_languages(self,count_value):
        freq_dict = {}
        length = self.df.shape[0]
        for index in range(length):
            test_str = self.df["Supported Languages"].values[index]
            test_str = test_str.replace("[", "").replace("]", "").replace("'","")
            language_list = test_str.split(",")
            for language in language_list:
                lang = language.strip().lower()
                if not lang in freq_dict.keys(): # dil dict'te yok ise 1 ile başlat.
                    freq_dict.update({lang:1})
                else: #var ise mevcut durumunu 1 arttır.
                    freq_dict[lang] += 1
        # Sözlüğün öğelerini (anahtar ve değer çiftleri) bir liste halinde al
        items = freq_dict.items()
        # Değerlere göre büyükten küçüğe sırala
        sorted_items = sorted(items, key=lambda x: x[1], reverse=True)
        # verdiğimiz değere göre desteklenmiş diller.
        values_greater_than_count_value = [key.lower() for key, value in sorted_items if value > count_value]
        return values_greater_than_count_value

    def parse_supported_languages(self,common_languages_count_value):
        common_languages = self.get_common_languages(common_languages_count_value)
        length = self.df.shape[0]
        columns = []
        for language in common_languages:
            columns.append(f"does_support_{language}")
            self.df[f"does_support_{language}"] = np.zeros(length,dtype=int)
        for index in range(length):
            test_str = self.df["Supported Languages"].values[index]
            test_str = test_str.replace("[", "").replace("]", "").replace("'","")
            language_list = test_str.split(",")
            for language in language_list:
                lang = language.strip().lower()
                if lang in common_languages:
                    self.df.at[index,f"does_support_{lang}"] = 1

    def drop_nan_review_text(self):
        nan_indices = self.df.loc[self.df['review_text'].isna()].index
        self.df.drop(nan_indices, inplace = True)
        self.df.reset_index(drop=True, inplace = True)
        # return self.df

    def add_is_weekend(self):
        self.df['Release Date'] = pd.to_datetime(self.df['Release Date'])
        self.df["day_of_week"] = self.df["Release Date"].dt.day_name()
        self.df["is_weekend"] = self.df["day_of_week"].isin(['Saturday', 'Sunday']).astype(int)
        self.df.drop(["day_of_week"], axis = 1, inplace = True)
        # return self.df

    def split_year_month_day(self,column):
        # Yıl, ay ve gün sütunlarını oluşturun.
        self.df['year'] = self.df[column].dt.year
        self.df['month'] = self.df[column].dt.month
        self.df['day'] = self.df[column].dt.day
        self.df.drop(column,axis = 1,inplace = True)
    
    def add_seasons(self):
        self.df['Release Date'] = pd.to_datetime(self.df['Release Date'])
        self.split_year_month_day("Release Date")

        self.df['is_winter'] = 0
        self.df['is_spring'] = 0
        self.df['is_summer'] = 0
        self.df['is_autumn'] = 0

        # Mevsimlere göre uygun sütunlara True değerlerini atama
        self.df.loc[((self.df['month'] == 12) & (self.df['day'] >= 21)) | ((self.df['month'].isin([1, 2]))) | ((self.df['month'] == 3) & (self.df['day'] < 21)), 'is_winter'] = 1
        self.df.loc[((self.df['month'] == 3) & (self.df['day'] >= 21)) | ((self.df['month'].isin([4, 5]))) | ((self.df['month'] == 6) & (self.df['day'] < 21)), 'is_spring'] = 1
        self.df.loc[((self.df['month'] == 6) & (self.df['day'] >= 21)) | ((self.df['month'].isin([7, 8]))) | ((self.df['month'] == 9) & (self.df['day'] < 23)), 'is_summer'] = 1
        self.df.loc[((self.df['month'] == 9) & (self.df['day'] >= 23)) | ((self.df['month'].isin([10, 11]))) | ((self.df['month'] == 12) & (self.df['day'] < 21)), 'is_autumn'] = 1

        if (self.df["is_winter"].sum() + self.df["is_spring"].sum() + self.df["is_summer"].sum() + self.df["is_autumn"].sum()) == self.df.shape[0]:
            print("seasons sorunsuz bir şekilde eklendi.")
        else:
            print("seasons kısmında bir sorun var.")

    def clean_reviews_tags(self,drop_reviews = False):
        if drop_reviews:
            indices = self.df.loc[self.df['review_text'].str.contains('reviews', case=False, na=False)].index
            self.df.drop(indices, inplace = True)
            self.df.reset_index(drop=True, inplace = True)
        else:
            cleaned_df_copy = self.df.copy()
            indices = cleaned_df_copy.loc[cleaned_df_copy['review_text'].str.contains('reviews', case=False, na=False)].index
            cleaned_df_copy.drop(indices, inplace = True)
            cleaned_df_copy.reset_index(drop=True, inplace = True)
            value_counts = cleaned_df_copy['review_text'].value_counts()

            # Oranları hesapla
            ratios = value_counts / len(cleaned_df_copy)
            review_texts = ratios.index.tolist()
            probabilities = ratios.values.tolist()
            indices = self.df.loc[self.df['review_text'].str.contains('reviews', case=False, na=False)].index
            for idx in indices:
                choice = np.random.choice(review_texts, p=probabilities)
                self.df.at[idx, 'review_text'] = choice

    def find_invalid_indices(self, df, column):
        invalid_indices = []
        for idx, value in enumerate(df[column]):
            try:
                float(value)
            except ValueError:
                invalid_indices.append(idx)
        return invalid_indices

    def clean_original_price(self):
        self.df["original_price"] = self.df["original_price"].str.strip("$")
        self.df["original_price"] = self.df["original_price"].str.replace(",", "")
        self.df["original_price"] = self.df["original_price"].str.replace("USD", "")
        self.df["original_price"] = self.df["original_price"].str.replace("Free To Play", "Free")
        self.df["original_price"] = self.df["original_price"].str.replace("Free to Play", "Free")
        self.df["original_price"] = self.df["original_price"].str.replace("Free", "0")
        invalid_indices = self.find_invalid_indices(self.df,"original_price")
        self.df = self.df.drop(invalid_indices).reset_index(drop=True)
        self.df = self.df.dropna(subset=['original_price']).reset_index(drop=True)
        self.df['original_price'] = self.df['original_price'].astype(float)

    def convert_storage_and_memory_to_gb(self):
        self.df['Memory_gb'] = self.df['Memory_mb'] / 1024
        self.df['Storage_gb'] = self.df['Storage_mb'] / 1024
        self.df.drop(columns=['Memory_mb', 'Storage_mb'], inplace=True)

    def check_outliers(self):
        outlier_indices = []
        
        ## memory_gb 500'den büyük olanları dropla.
        self.df = self.df[(self.df['Memory_gb'] < 500) & (self.df['Storage_gb'] < 1000)]
        # ###
        if len(self.df.loc[self.df["original_price"] > 500]) > 0:
            indices = self.df.loc[self.df["original_price"] > 500].index
            outlier_indices.extend(indices)
        outlier_indices = list(set(outlier_indices))

        self.df = self.df.drop(outlier_indices)
        self.df.reset_index(drop=True, inplace = True)

    def run_preprocessor(self,game_types_count_value,supported_tags_count_value,common_languages_count_value,drop_reviews):
        ##
        self.drop_nan_review_text()
        ##
        self.add_is_weekend()
        ##
        self.add_seasons()
        # game_types_count_value = 5000 #5000
        self.parse_game_features(game_types_count_value)
        # supported_tags_count_value = 15000
        self.parse_supported_tags(supported_tags_count_value)
        ##
        # common_languages_count_value = 5000
        self.parse_supported_languages(common_languages_count_value)
        ##
        self.clean_reviews_tags(drop_reviews)
        ##
        ##
        self.clean_original_price()
        dropped_columns = ["Link","Game Description","Recent Reviews Summary","All Reviews Summary","Recent Reviews Number","All Reviews Number","Developer","Publisher","review_value","Supported Languages","Popular Tags","Game Features","Minimum Requirements","Title"]
        self.df.drop(columns=dropped_columns, inplace = True)

        self.convert_storage_and_memory_to_gb()
        self.check_outliers()

        return self.df